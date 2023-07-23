# encoding: utf-8
require 'oci'
require 'zlib'
require 'stringio'
require "stud/interval"
require "logstash/namespace"
require "logstash/inputs/base"


class ObjectStorageGetter
  attr_reader :queue

  def archieve_object(_object)
      if @archieve_after_read
          @client.update_object_storage_tier(
              namespace=@namespace,
              bucket_name=@bucket_name,
              update_object_storage_tier_details={
                  :objectName => _object.name,
                  :storageTier => "Archive"
              }
          )
      end
  end

  def decode_gzip_file(_object, response)
      unless _object.name.match /gz(ip)?/i
          return response.data.to_s
      end
      
      buffer = StringIO.new(response.data.to_s)
      raw = Zlib::GzipReader.new(buffer)
      return raw.read
  end

    def process_data(raw_data, _object)
        raw_data.split("\n").each do |line|
            log = {:@metadata => JSON.parse _object.to_json}
            if @codec == "json"
                log["data"] = JSON.parse(line)
            else
                log["message"] = line
            end
            event = LogStash::Event.new(log)
            @queue << event
        end
    end

  def download_file(_object)
      response = @client.get_object(
          namespace=@namespace,
          bucket_name=@bucket_name,
          object_name=_object.name
      )
      raw_data = decode_gzip_file _object, response
      process_data raw_data, _object
      archieve_object _object
  end

  def filter_files(response)
      _buffer = []
      response.data.objects.each do |object|
          unless object.storage_tier == "Archieve" or object.archival_state == "Archived"
              #_buffer << object
              download_file object
          end
      end
      # return _buffer
  end


  def get_files(prefix="", _start="", _buffer=[])
      response = @client.list_objects(
          namespace=@namespace,
          bucket_name=@bucket_name,
          opts={
              :prefix => prefix,
              :start => _start,
              :fields => "name,timeCreated,timeModified,storageTier,archivalState"
          }
      )

      files = filter_files(response)
      _buffer.push(*files)
      next_start = response.data.next_start_with
      return (next_start == @last_call) ? _buffer : get_files(prefix, next_start, _buffer)
  end

  def initialize(namespace, bucket_name, credentials, queue, codec, archieve_after_read=true, filter_strategy="archive")
      @queue = queue
      @codec = codec
      @namespace = namespace
      @bucket_name = bucket_name
      @client = OCI::ObjectStorage::ObjectStorageClient.new(config: credentials)
      @archieve_after_read = archieve_after_read
      @filter_strategy = filter_strategy
  end

end


class LogStash::Inputs::OciObjectStorage < LogStash::Inputs::Base
  config_name "oci_object_storage"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  # The message string to use in the event.
  config :namespace, :validate => :string, :required => true
  config :bucket_name, :validate => :string, :required => true
  config :config_path, :validate => :string, :required => true
  config :archieve_after_read, :validate => :boolean, :default => true

  # Set how frequently messages should be sent.
  # The default, `1`, means send a message every second.
  config :interval, :validate => :number, :default => 30

  public
  def register
  end # def register

  def run(queue)
    @logstash_queue = queue

    config = OCI::ConfigFileLoader.load_config(config_file_location: @config_path)
    client = ObjectStorageGetter.new(
      namespace=@namespace,
      bucket_name=@bucket_name,
      credentials=config,
      queue=queue,
      codec=@codec,
      archieve_after_read=@archieve_after_read,
      filter_strategy="archive"
    )


    while !stop?
      client.get_files()
      Stud.stoppable_sleep(@interval) { stop? }
    end # loop
  end # def run

  def stop
    return true
  end
end # class LogStash::Inputs::Example
