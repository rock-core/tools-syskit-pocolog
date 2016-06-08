$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'syskit/test/self'
require 'syskit/pocolog'
require 'minitest/autorun'

module Syskit::Pocolog
    module Test
        attr_reader :created_log_dir
        attr_reader :all_log_files
        attr_reader :created_log_file

        def created_log_path
            created_log_file.io.first.path
        end

        def setup
            @pocolog_log_level = ::Pocolog.logger.level
            @all_log_files = Array.new
            ::Pocolog.logger.level = Logger::WARN
            app.register_app_extension('syskit-pocolog', Syskit::Pocolog::Plugin)
            super
        end

        def teardown
            all_log_files.each do |logfile|
                logfile.close
            end

            if @pocolog_log_level
                ::Pocolog.logger.level = @pocolog_log_level
            end
            if created_log_dir
                created_log_dir.rmtree
            end
            super
        end

        # Create a temporary directory to be used by the other log-related
        # helpers
        def create_log_dir
            @created_log_dir ||= Pathname.new(Dir.mktmpdir('syskit-pocolog-test'))
        end

        # Create a log file in a temporary directory
        #
        # @return [Pathname,Pocolog::Logfiles] the path to the file and the
        #   file object
        def create_log_file(filename, *typenames)
            create_log_dir
            path = created_log_dir + filename

            registry = Typelib::Registry.new

            begin path.sub_ext('.0.idx').unlink
            rescue Errno::ENOENT
            end
            @created_log_file = ::Pocolog::Logfiles.create(path.to_s, registry)
            @all_log_files << created_log_file
            return path.sub_ext(".0.log"), created_log_file
        end

        # Write all pending changes done to {#created_log_file} on disk
        def flush_log_file
            all_log_files.each do |logfile|
                logfile.io.each(&:flush)
            end
        end

        # Create a log stream on the last file created with
        # {#create_log_file}
        def create_log_stream(name, typename, metadata = Hash.new)
            if typename.respond_to?(:to_str)
                registry = Typelib::Registry.new
                type = registry.create_null typename
            else type = typename
            end
            created_log_file.create_stream name, type, metadata
        end
    end
end
Minitest::Test.include Syskit::Pocolog::Test
