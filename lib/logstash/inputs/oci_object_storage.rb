# frozen_string_literal: true

require 'oci'
require 'zlib'
require 'date'
require 'tmpdir'
require 'stringio'
require 'stud/interval'
require 'logstash/namespace'
require 'logstash/inputs/base'

# The ObjectStorageGetter class is a utility class used by the OciObjectStorage Logstash input
# plugin to interact with Oracle Cloud Infrastructure (OCI) Object Storage.
# It provides methods to retrieve, process, and archive files from a specified OCI Object Storage bucket.
class ObjectStorageGetter
  attr_reader :sincedb_time

  def archieve_object(object)
    return unless @archieve_after_read

    @client.update_object_storage_tier(
      @namespace,
      @bucket_name,
      {
        objectName: object.name,
        storageTier: 'Archive'
      }
    )
  end

  def decode_gzip_file(object, response)
    return response.data.to_s unless object.name.match(/gz(ip)?/i)

    buffer = StringIO.new(response.data.to_s)
    raw = Zlib::GzipReader.new(buffer)
    raw.read
  end

  def process_data(raw_data, object)
    meta = JSON.parse object.to_hash.to_json
    @codec.decode(raw_data) do |event|
      event.set('[@metadata][oci][object_storage]', meta)
    end
    @codec.flush do |event|
        @queue << event
    end
  end

  def download_file(object)
    response = @client.get_object(
      @namespace,
      @bucket_name,
      object.name
    )
    raw_data = decode_gzip_file object, response
    process_data raw_data, object
    archieve_object object if @filter_strategy == 'archive'
  end

  def download_filtered_files(buffer)
    time_buffer = []
    buffer.each do |object|
      normalized_time = Time.parse(object.time_modified.to_s)
      next if (['Archive'].include? object.storage_tier) || (["Restoring", "Archived"].include? object.archival_state)
      next if @sincedb_time > normalized_time

      time_buffer << normalized_time
      @logger.info("Downloading file from #{object.name}")
      begin
        download_file object
      rescue OCI::Errors::ServiceError => error
        @logger.warn("The file #{object.name} cannot be downloaded: #{error} => #{object.to_hash}")
      end
    end
    @sincedb_time = time_buffer.max unless time_buffer.empty?
  end

  def retrieve_files_recursive(parameters, buffer=[])
    response = @client.list_objects(@namespace, @bucket_name, parameters)
    buffer.push(*response.data.objects)

    if response.data.next_start_with.nil?
      @logger.debug('Nil pointer received!')
    else
      @logger.info("Retriving next page: Last Page: #{@next_start} - Next Page: #{response.data.next_start_with}")
      @next_start = response.data.next_start_with
      parameters[:start] = @next_start
      return retrieve_files_recursive(parameters, buffer)
    end
    return buffer
  end

  def retrieve_files(prefix = '')
    parameters = {
      prefix: prefix,
      start: @next_start || '',
      fields: 'name,timeCreated,timeModified,storageTier,archivalState'
    }
    buffer = retrieve_files_recursive parameters
    download_filtered_files buffer
  end

  def initialize(parameters)
    @queue = parameters[:queue]
    @codec = parameters[:codec]
    @namespace = parameters[:namespace]
    @bucket_name = parameters[:bucket_name]
    @client = OCI::ObjectStorage::ObjectStorageClient.new(config: parameters[:credentials])
    @archieve_after_read = parameters[:archieve_after_read]
    @filter_strategy = parameters[:filter_strategy]
    @sincedb_time = parameters[:sincedb_time] || Time.new(0)
    @logger = parameters[:logger]
    @threads = parameters[:threads]
    @next_start = ''
  end
end

module LogStash
  module Inputs
    # The OciObjectStorage class is a Logstash input plugin that enables Logstash to read data
    # from Oracle Cloud Infrastructure (OCI) Object Storage.
    # It periodically fetches files from a specified OCI Object Storage bucket and forwards
    # the data to the Logstash pipeline for further processing.
    class OciObjectStorage < LogStash::Inputs::Base
      config_name 'oci_object_storage'

      # If undefined, Logstash will complain, even if codec is unused.
      default :codec, 'plain'

      config :namespace, validate: :string, required: true
      config :bucket_name, validate: :string, required: true
      config :config_path, validate: :string, required: true

      config :archieve_after_read, validate: :boolean, required: false, default: true
      config :filter_strategy, validate: :string, required: false, default: 'archive'
      config :threads, validate: :number, required: false, default: ::LogStash::Config::CpuCoreStrategy.maximum
      config :interval, validate: :number, default: 30
      config :sincedb_path, validate: :string, default: nil

      def sincedb_file
        logstash_path = LogStash::SETTINGS.get_value('path.data')
        digest = Digest::MD5.hexdigest("#{@namespace}+#{@bucket_name}+#{@prefix}")
        dir = File.join(logstash_path, 'plugins', 'inputs', 'oci_object_storage')
        FileUtils.mkdir_p(dir)
        path = File.join(dir, "sincedb_#{digest}")
        if ENV['HOME']
          old = File.join(ENV['HOME'], ".sincedb_#{digest}")
          if File.exist?(old)
            logger.info('Migrating old sincedb in $HOME to {path.data}')
            FileUtils.mv(old, path)
          end
        end
        path
      end

      def register
        return unless filter_strategy == 'sincedb'

        if @sincedb_path.nil?
          @logger.info('Using default generated file for the sincedb', filename: sincedb_file)
          @sincedb = SinceDB::File.new(sincedb_file)
        else
          @logger.info('Using the provided sincedb_path', sincedb_path: @sincedb_path)
          @sincedb = SinceDB::File.new(@sincedb_path)
        end

        @sincedb_time = @sincedb.read
      end

      def run(queue)
        @logger.info("Loading config file: #{@config_path}")
        parameters = {
          namespace: @namespace,
          bucket_name: @bucket_name,
          credentials: OCI::ConfigFileLoader.load_config(config_file_location: @config_path),
          queue: queue,
          codec: @codec,
          archieve_after_read: @archieve_after_read,
          filter_strategy: @filter_strategy,
          sincedb_time: @sincedb_time,
          threads: @threads,
          logger: @logger
        }
        client = ObjectStorageGetter.new(parameters)
        until stop?
          client.retrieve_files
          @sincedb.write(client.sincedb_time) if filter_strategy == 'sincedb'
          Stud.stoppable_sleep(@interval) { stop? }
        end
      end

      def stop
        true
      end
    end
  end
end

module SinceDB
  # Foo
  class File
    def initialize(file)
      @sincedb_path = file
    end

    # @return [Time]
    def read
      if ::File.exist?(@sincedb_path)
        content = ::File.read(@sincedb_path).chomp.strip
        # If the file was created but we didn't have the time to write to it
        content.empty? ? Time.new(0) : Time.parse(content)
      else
        Time.new(0)
      end
    end

    def write(since = nil)
      since = Time.now if since.nil?
      ::File.open(@sincedb_path, 'w') { |file| file.write(since.to_s) }
    end
  end
end
