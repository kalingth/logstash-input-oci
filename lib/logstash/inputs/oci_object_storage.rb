# frozen_string_literal: true

require 'oci'
require 'zlib'
require 'date'
require 'tmpdir'
require 'stringio'
require 'thread/pool'
require 'stud/interval'
require 'logstash/namespace'
require 'logstash/inputs/base'

# The ObjectStorageGetter class is a utility class used by the OciObjectStorage Logstash input
# plugin to interact with Oracle Cloud Infrastructure (OCI) Object Storage.
# It provides methods to retrieve, process, and archive files from a specified OCI Object Storage bucket.
class ObjectStorageGetter
  attr_reader :sincedb_time

  # Archive an object in the Oracle Cloud Infrastructure (OCI) Object Storage.
  # The archiving is performed based on the configuration specified in the `@archieve_after_read` attribute
  # of the `ObjectStorageGetter` class.
  #
  # @param object [Object] The object containing metadata information. It represents a file in the OCI Object Storage.
  def archieve_object(object)
    return unless @archieve_after_read

    parmeters = {
      objectName: object.name,
      storageTier: 'Archive'
    }
    @client.update_object_storage_tier(@namespace, @bucket_name, parmeters)
  end

  # Decodes the data of the given object using gzip decompression if the file has a `.gz` or `.gzip` extension.
  # If the object's name does not match the pattern for gzip files, the data is returned as-is without decompression.
  #
  # @param object [Object] The object containing metadata information. It represents a file in the OCI Object Storage.
  # @param response [Object] The response object containing the data of the object retrieved from the Object Storage.
  # @return [String] The decoded data as a String.
  def decode_gzip_file(object, response)
    return response.data.to_s unless object.name.match(/gz(ip)?/i)

    buffer = StringIO.new(response.data.to_s)
    raw = Zlib::GzipReader.new(buffer)
    raw.read
  end

  # Process the raw data using the specified Logstash codec and add metadata from the object to each event.
  # After, includes this event on Logstash Queue.
  #
  # @param raw_data [String] The raw data to be processed. It represents the content of a file.
  # @param object [Object] The object containing metadata information. It represents a file in the OCI Object Storage.
  def process_data(raw_data, object)
    meta = JSON.parse object.to_hash.to_json
    @codec.decode(raw_data) do |event|
      event.set('[@metadata][oci][object_storage]', meta)
      @queue << event
    end
  end

  # Download and process the data of the given object from the OCI Object Storage bucket.
  #
  # @param object [Object] The object containing metadata information. It represents a file in the OCI Object Storage.
  def download_file(object)
    response = @client.get_object(
      @namespace,
      @bucket_name,
      object.name
    )
    raw_data = decode_gzip_file object, response
    process_data raw_data, object
    archieve_object object if @filter_strategy == 'archive'
  rescue OCI::Errors::ServiceError => e
    @logger.warn("The file #{object.name} cannot be downloaded: #{e} => #{object.to_hash}")
  end

  # Check whether the given object should be skipped or processed based on specific conditions.
  #
  # @param object [Object] The object containing metadata information. It represents a file in the OCI Object Storage.
  # @param normalized_time [Time] The normalized time of the object's modification timestamp used for comparison.
  def skip_file?(object, normalized_time)
    if ['Archive'].include? object.storage_tier
      true
    elsif %w[Restoring Archived].include? object.archival_state
      true
    elsif (@sincedb_time > normalized_time) && (@filter_strategy == 'sincedb')
      true
    else
      false
    end
  end

  # Download and process filtered files from the OCI Object Storage bucket.
  # Download and processing of objects are performed concurrently using a thread pool, with the number of threads
  # specified by the `@threads` attribute of the `ObjectStorageGetter` class.
  #
  # @param buffer [Array] An array containing objects to be downloaded and processed. Each object represents a file.
  def download_filtered_files(buffer)
    time_buffer = []
    pool = Thread.pool(@threads)
    buffer.each do |object|
      normalized_time = Time.parse(object.time_modified.to_s)
      next if skip_file? object, normalized_time

      time_buffer << normalized_time
      @logger.info("Downloading file from #{object.name}")
      pool.process { download_file object }
    end
    pool.shutdown
    @sincedb_time = time_buffer.max unless time_buffer.empty? && (@filter_strategy == 'sincedb')
  end

  # Retrieve files from the OCI Object Storage bucket recursively based on specified parameters.
  #
  # @param parameters [Hash] A hash containing parameters for the OCI Object Storage API request.
  # @param buffer [Array] An optional array that accumulates the retrieved objects. Each object represents a file.
  # @return [Array] An array containing the retrieved objects after the recursive retrieval process.
  def retrieve_files_recursive(parameters, buffer = [])
    response = @client.list_objects(@namespace, @bucket_name, parameters)
    buffer.push(*response.data.objects)
    if response.data.next_start_with.nil?
      @logger.debug('Nil pointer received!')
      return buffer
    end

    @logger.info("Retriving next page: Last Page: #{@next_start} - Next Page: #{response.data.next_start_with}")
    @next_start = response.data.next_start_with
    parameters[:start] = @next_start
    retrieve_files_recursive(parameters, buffer)
  end

  # Retrieve files from the OCI Object Storage bucket based on the specified prefix.
  #
  # @param prefix [String] An optional parameter to filter files based on that name prefix. Default is an empty string.
  def retrieve_files(prefix = '')
    parameters = {
      prefix: prefix,
      start: @next_start || '',
      fields: 'name,timeCreated,timeModified,storageTier,archivalState'
    }
    buffer = retrieve_files_recursive parameters
    download_filtered_files buffer
  end

  # Initialize a new instance of the ObjectStorageGetter class with the given parameters.
  #
  # Parameters:
  # @param parameters [Hash] A hash containing configuration parameters for the ObjectStorageGetter instance.
  #   - :queue [LogStash::Util::WrappedSynchronousQueue] The queue to which the processed events will be sent.
  #   - :codec [Codec] The codec used to decode data from the retrieved objects in the OCI Object Storage bucket.
  #   - :namespace [String] The OCI Object Storage namespace associated with the bucket.
  #   - :bucket_name [String] The name of the OCI Object Storage bucket.
  #   - :credentials [OCI::ConfigFile::Config] The OCI configuration used for authentication.
  #   - :archieve_after_read [Boolean] A flag indicating whether the retrieved objects should be archived after read.
  #   - :filter_strategy [String] Strategy used for filtering files during retrieval. Can be `archive` or `sincedb`.
  #   - :sincedb_time [Time] Last modified timestamp recorded in the sincedb for filtering based on the 'sincedb'.
  #   - :logger [Logger] Logger instance used to log information, warnings, and errors during the retrieval process.
  #   - :threads [Integer] The number of threads used for concurrent processing of the retrieved objects.
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

# The LogStash module provides a namespace for all Logstash-related functionalities and classes.
# It encapsulates the components required for data ingestion, processing, and output in the Logstash pipeline.
# The module contains various submodules and classes that define inputs, filters, codecs, and outputs.
# These components work together to collect, filter, and transform data from various sources, enabling data
# ingestion into the Logstash pipeline.
module LogStash
  # The LogStash::Inputs module is a submodule of the LogStash module, specifically responsible for handling data
  # input in the Logstash pipeline. It provides a namespace for all input-related functionalities, including various
  # input plugins that enable Logstash to collect data from different sources. Each input plugin represents a specific
  # data source or input type, such as files, databases, HTTP endpoints, message queues, and more.
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

      # Get the folder path for the sincedb file used by the OCI Object Storage input plugin in Logstash.
      #
      # @return [String] The folder path for the sincedb file specific to the OCI Object Storage input plugin.
      def sincedb_folder
        logstash_path = LogStash::SETTINGS.get_value('path.data')
        dir = File.join(logstash_path, 'plugins', 'inputs', 'oci_object_storage')
        FileUtils.mkdir_p(dir)
        dir
      end

      # Get the full path to the sincedb file for the OCI Object Storage input plugin in Logstash.
      #
      # @return [String] The full path to the sincedb file specific to the OCI Object Storage input plugin.
      def sincedb_file
        digest = Digest::MD5.hexdigest("#{@namespace}+#{@bucket_name}+#{@prefix}")
        path = File.join(sincedb_folder, "sincedb_#{digest}")
        if ENV['HOME']
          old = File.join(ENV['HOME'], ".sincedb_#{digest}")
          if File.exist?(old)
            logger.info('Migrating old sincedb in $HOME to {path.data}')
            FileUtils.mv(old, path)
          end
        end
        path
      end

      # The register method is a lifecycle method in the OCIObjectStorage input plugin for Logstash.
      # It is called once during the plugin initialization phase and is responsible for setting up sincedb
      # functionality when the 'sincedb' filter strategy is enabled.
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

      # The run method is a lifecycle method in the OCIObjectStorage input plugin for Logstash.
      # It is responsible for the main execution of the OCIObjectStorage input plugin and the retrieval and
      # processing of files from the OCI Object Storage bucket.
      #
      # @param queue [LogStash::Util::WrappedSynchronousQueue] The queue to which the processed events will be sent.
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
        @client = ObjectStorageGetter.new(parameters)
        until stop?
          @client.retrieve_files
          @sincedb.write(@client.sincedb_time) if filter_strategy == 'sincedb'
          Stud.stoppable_sleep(@interval) { stop? }
        end
      end

      # The stop method is a lifecycle method in the OCIObjectStorage input plugin for Logstash.
      # It is responsible for stopping the execution of the OCIObjectStorage input plugin and performing
      # any necessary cleanup tasks before exiting.
      #
      # @return [Boolean] Always returns true to signal that the Logstash pipeline should be stopped.
      def stop
        @sincedb.write(@client.sincedb_time) if filter_strategy == 'sincedb'
        true
      end
    end
  end
end

# The SinceDB module provides a namespace for classes related to managing since database (sincedb) functionality.
# The sincedb is a feature used to keep track of the progress in reading and processing files, ensuring that
# Logstash resumes where it left off in case of interruptions or restarts. The module contains classes and methods
# that handle reading and writing sincedb files, storing information about the last processed position in a file,
# and updating it as new data is ingested.
module SinceDB
  # The SinceDB::File class represents a sincedb file used to track the progress of reading and processing files
  # in Logstash. It provides methods to read and write sincedb information, which helps to maintain data integrity
  # and support incremental data processing for file-based data sources in Logstash.
  class File
    # Constructor method to initialize a new instance of SinceDB::File.
    #
    # @param file [String] The path to the sincedb file to be managed by this instance.
    def initialize(file)
      @sincedb_path = file
    end

    # Read the sincedb file and return the last processed timestamp as a Time object.
    #
    # @return [Time] The last processed timestamp recorded in the sincedb file, represented as a Time object.
    def read
      if ::File.exist?(@sincedb_path)
        content = ::File.read(@sincedb_path).chomp.strip
        # If the file was created but we didn't have the time to write to it
        content.empty? ? Time.new(0) : Time.parse(content)
      else
        Time.new(0)
      end
    end

    # Write the provided timestamp to the sincedb file.
    #
    # @param since [Time] An optional parameter representing the timestamp. If not provided, Time.now is used.
    def write(since = nil)
      since = Time.now if since.nil?
      ::File.open(@sincedb_path, 'w') { |file| file.write(since.to_s) }
    end
  end
end
