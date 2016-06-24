require 'test_helper'

module Syskit::Pocolog
    describe Datastore do
        attr_reader :root_path, :datastore_path, :datastore

        before do
            @root_path = Pathname.new(Dir.mktmpdir)
            @datastore_path = root_path + 'datastore'
            datastore_path.mkpath
            @datastore = Datastore.new(datastore_path)
        end
        after do
            root_path.rmtree
        end
        describe "#in_incoming" do
            it "creates an incoming directory in the datastore and yields it" do
                datastore.in_incoming do |path|
                    assert_equal (datastore_path + 'incoming' + '0'), path
                    assert path.directory?
                end
            end
            it "handles having another process create a path concurrently" do
                (datastore_path + "incoming").mkpath
                called = false
                flexmock(Pathname).new_instances.should_receive(:mkdir).
                    and_return do
                        if !called
                            called = true
                            raise Errno::EEXIST
                        end
                    end

                datastore.in_incoming do |path|
                    assert_equal (datastore_path + 'incoming' + '1'), path
                end
            end
            it "ignores existing paths" do
                (datastore_path + "incoming" + "0").mkpath
                datastore.in_incoming do |path|
                    assert_equal (datastore_path + 'incoming' + '1'), path
                end
            end
            it "deletes the created path if it still exists at the end of the block" do
                created_path = datastore.in_incoming do |path|
                    path
                end
                refute created_path.exist?
            end
            it "does nothing if the path does not exist anymore at the end of the block" do
                created_path = datastore.in_incoming do |path|
                    FileUtils.mv path, (root_path + "safe")
                end
                assert (root_path + "safe").exist?
            end
        end

        describe "#has?" do
            attr_reader :digest
            before do
                @digest = Dataset.string_digest('exists')
                (datastore_path + digest).mkpath
            end

            it "returns false if there is no folder with the dataset digest in the store" do
                refute datastore.has?(Dataset.string_digest('does_not_exist'))
            end
            it "returns true if there is a folder with the dataset digest in the store" do
                assert datastore.has?(digest)
            end
        end

        describe "#delete" do
            attr_reader :digest, :dataset_path
            before do
                @digest = Dataset.string_digest('exists')
                @dataset_path = datastore.path_of(digest)
                dataset_path.mkpath
            end

            it "deletes the dataset's path and its contents" do
                FileUtils.touch dataset_path + "file"
                datastore.delete(digest)
                assert !dataset_path.exist?
            end
        end

        describe "#get" do
            attr_reader :digest, :dataset_path
            before do
                @digest = Dataset.string_digest('exists')
                @dataset_path = datastore.path_of(digest)
                dataset_path.mkpath
                dataset = Dataset.new(dataset_path)
                dataset.write_dataset_identity_to_metadata_file
                dataset.metadata_write_to_file
            end

            it "returns a Dataset object pointing to the path" do
                dataset = datastore.get(digest)
                assert_kind_of Dataset, dataset
                assert_equal dataset_path, dataset.dataset_path
            end

            it "raises ArgumentError if the dataset does not exist" do
                assert_raises(ArgumentError) do
                    datastore.get(Dataset.string_digest("does_not_exist"))
                end
            end
        end
    end
end


