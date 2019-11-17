$LOAD_PATH.unshift File.expand_path('../lib', __dir__)
require 'syskit/test/self'
require 'syskit/log'
require 'pocolog'
require 'pocolog/test_helpers'
require 'minitest/autorun'
require 'syskit/log/datastore/index_build'

module Syskit::Log
    module Test
        include Pocolog::TestHelpers

        def setup
            @pocolog_log_level = Pocolog.logger.level
            Pocolog.logger.level = Logger::WARN
            unless Roby.app.loaded_plugin?('syskit-log')
                Roby.app.add_plugin('syskit-log', Syskit::Log::Plugin)
            end

            super
        end

        def teardown
            Pocolog.logger.level = @pocolog_log_level if @pocolog_log_level
            super
        end

        def logfile_pathname(*basename)
            Pathname.new(logfile_path(*basename))
        end

        def create_datastore(path)
            @datastore = Datastore.create(path)
        end

        def create_dataset(digest, metadata: Hash.new)
            if !@datastore
                raise ArgumentError, "must call #create_datastore before #create_dataset"
            end

            core_path = @datastore.core_path_of(digest)
            core_path.mkpath
            move_logfile_path(core_path + "pocolog", delete_current: false)
            dataset = Datastore::Dataset.new(core_path, cache: @datastore.cache_path_of(digest))
            if block_given?
                begin
                    yield
                ensure
                    identity = dataset.compute_dataset_identity_from_files
                    dataset.write_dataset_identity_to_metadata_file(identity)
                    metadata.each do |k, v|
                        dataset.metadata_set(k, *v)
                    end
                    dataset.metadata_write_to_file
                    Datastore.index_build(datastore, dataset)
                end
            else
                dataset
            end
        end

        # Create a stream in a normalized dataset
        def create_normalized_stream(name, type: int32_t, metadata: Hash.new)
            logfile_basename = name.gsub('/', ':').gsub(/^:/, '') + ".0.log"
            logfile_path = Pathname.new(logfile_path(logfile_basename))
            create_logfile logfile_basename do
                stream = create_logfile_stream(name, type: type, metadata: metadata)
                yield if block_given?
                flush_logfile
                registry_checksum = Streams.save_registry_in_normalized_dataset(logfile_path, stream)
                Streams.update_normalized_metadata(logfile_pathname) do |metadata|
                    metadata << Streams.create_metadata_entry(logfile_path, stream, registry_checksum)
                end
            end
            return logfile_path
        end
    end
end
Minitest::Test.include Syskit::Log::Test
