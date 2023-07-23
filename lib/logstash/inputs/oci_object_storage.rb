# frozen_string_literal: true

require 'oci'
require 'zlib'
require 'stringio'
require 'stud/interval'
require 'logstash/namespace'
require 'logstash/inputs/base'

# The ObjectStorageGetter class is a utility class used by the OciObjectStorage Logstash input
# plugin to interact with Oracle Cloud Infrastructure (OCI) Object Storage.
# It provides methods to retrieve, process, and archive files from a specified OCI Object Storage bucket.
class ObjectStorageGetter
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
      event.set('[@metadata]', meta)
      @queue << event
    end
    @codec.flush
  end

  def download_file(object)
    response = @client.get_object(
      @namespace,
      @bucket_name,
      object.name
    )
    raw_data = decode_gzip_file object, response
    process_data raw_data, object
    archieve_object object
  end

  def download_filtered_files(response)
    _buffer = []
    response.data.objects.each do |object|
      download_file object unless (object.storage_tier == 'Archieve') || (object.archival_state == 'Archived')
    end
  end

  def retrieve_files(prefix = '', start = '')
    parmeters = {
      prefix: prefix,
      start: start,
      fields: 'name,timeCreated,timeModified,storageTier,archivalState'
    }
    response = @client.list_objects(@namespace, @bucket_name, parmeters)
    download_filtered_files(response)
    next_start = response.data.next_start_with
    retrieve_files(prefix, next_start) unless next_start == @last_call
  end

  def initialize(parameters)
    @queue = parameters[:queue]
    @codec = parameters[:codec]
    @namespace = parameters[:namespace]
    @bucket_name = parameters[:bucket_name]
    @client = OCI::ObjectStorage::ObjectStorageClient.new(config: parameters[:credentials])
    @archieve_after_read = parameters[:archieve_after_read]
    @filter_strategy = parameters[:filter_strategy]
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

      def register; end

      def run(queue)
        @logstash_queue = queue

        @logger.info("Loading config file: #{@config_path}")
        parameters = {
          namespace: @namespace,
          bucket_name: @bucket_name,
          credentials: OCI::ConfigFileLoader.load_config(config_file_location: @config_path),
          queue: @queue,
          codec: @codec,
          archieve_after_read: @archieve_after_read,
          filter_strategy: @filter_strategy
        }
        client = ObjectStorageGetter.new(parameters)

        until stop?
          client.retrieve_files
          Stud.stoppable_sleep(@interval) { stop? }
        end
      end

      def stop
        true
      end
    end
  end
end
