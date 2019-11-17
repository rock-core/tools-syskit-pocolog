require 'roby/droby/logfile/index'

module Syskit::Log
    class Datastore
        def self.index_build(datastore, dataset, force: false, reporter: Pocolog::CLI::NullReporter.new)
            IndexBuild.new(datastore, dataset).rebuild(force: force, reporter: reporter)
        end

        # Builds the index information for a given dataset in a store
        #
        # It builds dataset-local indexes and then updates the global store
        # index
        class IndexBuild
            # The dataset we're indexing
            attr_reader :dataset
            # The datastore whose index we'll be updating
            attr_reader :datastore

            def initialize(datastore, dataset)
                @datastore = datastore
                @dataset   = dataset
            end

            # Rebuild this dataset's indexes
            def rebuild(force: false, reporter: Pocolog::CLI::NullReporter.new)
                rebuild_pocolog_indexes(force: force, reporter: reporter)
                rebuild_roby_index(force: force, reporter: reporter)
            end

            # Rebuild the dataset's pocolog indexes
            #
            # @param [Boolean] force if true, the indexes will all be rebuilt.
            #   Otherwise, only the indexes that do not seem to be up-to-date
            #   will.
            def rebuild_pocolog_indexes(force: false, reporter: Pocolog::CLI::NullReporter.new)
                pocolog_index_dir = (dataset.cache_path + "pocolog")
                pocolog_index_dir.mkpath
                if force
                    # Just delete pocolog/*.idx from the cache
                    Pathname.glob(pocolog_index_dir + "*.idx") do |p|
                        p.unlink
                    end
                end

                dataset.each_pocolog_path do |logfile_path|
                    logfile_name = logfile_path.relative_path_from(dataset.dataset_path)
                    begin
                        index_path = Pocolog::Logfiles.default_index_filename(
                            logfile_path, index_dir: pocolog_index_dir)

                        stat = logfile_path.stat
                        begin
                            File.open(index_path) do |index_io|
                                Pocolog::Format::Current.read_index_stream_info(index_io, expected_file_size: stat.size)
                            end
                            reporter.log "  up-to-date: #{logfile_name}"

                        rescue Errno::ENOENT, Pocolog::InvalidIndex => e
                            reporter.log "  rebuilding: #{logfile_name}"
                            logfile_path.open do |logfile_io|
                                Pocolog::Format::Current.rebuild_index_file(logfile_io, index_path)
                            end
                        end
                    end
                end
            end

            # Rebuild the dataset's Roby index
            def rebuild_roby_index(force: false, reporter: Pocolog::CLI::NullReporter.new)
                dataset.cache_path.mkpath
                roby_log_path   = dataset.dataset_path + "roby-events.log"
                if !roby_log_path.exist?
                    return
                end

                roby_index_path = dataset.cache_path + "roby-events.idx"
                if force || !Roby::DRoby::Logfile::Index.valid_file?(roby_log_path, roby_index_path)
                    reporter.log "  rebuilding: roby-events.log"
                    begin
                        Roby::DRoby::Logfile::Index.rebuild_file(roby_log_path, roby_index_path)
                    rescue Roby::DRoby::Logfile::InvalidFormatVersion
                        reporter.warn "  roby-events.log is an obsolete Roby log file format, skipping"
                    end
                else
                    reporter.log "  up-to-date: roby-events.log"
                end
            end
        end
    end
end

