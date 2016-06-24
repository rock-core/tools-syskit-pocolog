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

        # Whether a dataset with the given ID exists
        def has?(digest)
            path_of(digest).exist?
        end

        # Remove an existing dataset
        def delete(digest)
            path_of(digest).rmtree
        end

        # The full path to a dataset
        #
        # The dataset itself is not guaranteed to exist
        def path_of(digest)
            datastore_path + digest
        end

        # Get an existing dataset
        def get(digest)
            if !has?(digest)
                raise ArgumentError, "no dataset with digest #{digest} exist"
            end

            dataset = Dataset.new(path_of(digest))
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
                yield(import_dir)
            ensure
                if !keep && import_dir.exist?
                    import_dir.rmtree
                end
            end
        end

    end
end

