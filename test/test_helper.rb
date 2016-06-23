$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'syskit/test/self'
require 'syskit/pocolog'
require 'pocolog/test_helpers'
require 'minitest/autorun'

module Syskit::Pocolog
    module Test
        include Pocolog::TestHelpers

        def setup
            @pocolog_log_level = ::Pocolog.logger.level
            ::Pocolog.logger.level = Logger::WARN
            app.register_app_extension('syskit-pocolog', Syskit::Pocolog::Plugin)
            super
        end

        def teardown
            if @pocolog_log_level
                ::Pocolog.logger.level = @pocolog_log_level
            end
            super
        end

        def logfile_pathname(*basename)
            Pathname.new(logfile_path(*basename))
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
Minitest::Test.include Syskit::Pocolog::Test
