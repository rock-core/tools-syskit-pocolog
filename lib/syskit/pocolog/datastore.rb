module Syskit::Pocolog
    # A store for normalized datasets
    class Datastore
        # The store's path on disk
        #
        # @return [Pathname]
        attr_reader :datastore_path

        def initialize(datastore_path)
            @datastore_path = datastore_path.realpath
        end

        # Setup a directory structure for the given path to be a valid datastore
        def self.create(datastore_path)
            datastore_path.mkpath
            (datastore_path + "core").mkpath
            (datastore_path + "cache").mkpath
            (datastore_path + "incoming").mkpath
            store = Datastore.new(datastore_path)
        end

        # Whether a dataset with the given ID exists
        def has?(digest)
            core_path_of(digest).exist?
        end

        # Remove an existing dataset
        def delete(digest)
            core_path_of(digest).rmtree
            if cache_path_of(digest).exist?
                cache_path_of(digest).rmtree
            end
        end

        # The full path to a dataset
        #
        # The dataset itself is not guaranteed to exist
        def core_path_of(digest)
            datastore_path + "core" + digest
        end

        # The full path to a dataset
        #
        # The dataset itself is not guaranteed to exist
        def cache_path_of(digest)
            datastore_path + "cache" + digest
        end

        # Get an existing dataset
        def get(digest)
            if !has?(digest)
                raise ArgumentError, "no dataset with digest #{digest} exist"
            end

            dataset = Dataset.new(core_path_of(digest), cache: cache_path_of(digest))
            dataset.weak_validate_identity_metadata
            dataset.metadata
            dataset
        end

        # @api private
        #
        # Create a working directory in the incoming dir of the data store and
        # yield
        #
        # The created dir is deleted if it still exists after the block
        # returned. This ensures that no incoming leftovers are kept if an
        # opeartion fails
        def in_incoming(keep: false)
            incoming_dir = (datastore_path + "incoming")
            incoming_dir.mkpath

            i = 0
            begin
                while (import_dir = (incoming_dir + i.to_s)).exist?
                    i += 1
                end
                import_dir.mkdir
            rescue Errno::EEXIST
                i += 1
                retry
            end

            begin
                core_path = import_dir + "core"
                cache_path = import_dir + "cache"
                core_path.mkdir
                cache_path.mkdir
                yield(import_dir + "core", import_dir + "cache")
            ensure
                if !keep && import_dir.exist?
                    import_dir.rmtree
                end
            end
        end

    end
end

