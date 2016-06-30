require 'roby/droby/logfile/index'

module Syskit::Pocolog
    class Datastore
        def self.index_build(datastore, dataset, force: false)
            IndexBuild.new(datastore, dataset).rebuild(force: force)
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
            def rebuild(force: false)
                rebuild_pocolog_indexes(force: force)
                rebuild_roby_index(force: force)
            end

            # Rebuild the dataset's pocolog indexes
            #
            # @param [Boolean] force if true, the indexes will all be rebuilt.
            #   Otherwise, only the indexes that do not seem to be up-to-date
            #   will.
            def rebuild_pocolog_indexes(force: false)
                pocolog_index_dir = (dataset.cache_path + "pocolog")
                pocolog_index_dir.mkpath
                if force
                    # Just delete pocolog/*.idx from the cache
                    Pathname.glob(pocolog_index_dir + "*.idx") do |p|
                        p.unlink
                    end
                end

                dataset.each_pocolog_stream.to_a
            end

            # Rebuild the dataset's Roby index
            def rebuild_roby_index(force: false)
                dataset.cache_path.mkpath
                roby_log_path   = dataset.dataset_path + "roby-events.log"
                if !roby_log_path.exist?
                    return
                end

                roby_index_path = dataset.cache_path + "roby-events.idx"
                if force || !Roby::DRoby::Logfile::Index.valid_file?(roby_log_path, roby_index_path)
                    Roby::DRoby::Logfile::Index.rebuild_file(roby_log_path, roby_index_path)
                end
            end
        end
    end
end

