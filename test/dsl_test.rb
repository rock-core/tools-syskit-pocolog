# frozen_string_literal: true

require "test_helper"
require "syskit/log/dsl"
require "daru"

module Syskit
    module Log # :nodoc:
        describe DSL do
            before do
                @__default_store = ENV["SYSKIT_LOG_STORE"]
                ENV.delete("SYSKIT_LOG_STORE")

                @root_path = Pathname.new(Dir.mktmpdir)
                @datastore_path = @root_path + "datastore"
                create_datastore(@datastore_path)
            end

            after do
                @root_path.rmtree
                if @__default_store
                    ENV["SYSKIT_LOG_STORE"] = @__default_store
                else
                    ENV.delete("SYSKIT_LOG_STORE")
                end
            end

            describe "datastore selection" do
                it "initializes the datastore to the default if there is one" do
                    ENV["SYSKIT_LOG_STORE"] = @datastore_path.to_s
                    assert_equal @datastore_path,
                                 make_context.datastore.datastore_path
                end

                it "does not initialize the datastore if the environment variable is unset" do
                    ENV.delete("SYSKIT_LOG_STORE")
                    assert_nil make_context.datastore
                end

                it "allows to explicit select one by path" do
                    ENV.delete("SYSKIT_LOG_STORE")
                    context = make_context
                    context.datastore_select @datastore_path
                    assert_equal @datastore_path,
                                 context.datastore.datastore_path
                end
            end

            describe "dataset selection" do
                before do
                    @dataset = create_dataset("exists") {}

                    @context = make_context
                    @context.datastore_select @datastore_path
                end

                it "selects it by complete digest" do
                    @context.dataset_select @dataset.digest
                    assert_equal @dataset.digest, @context.dataset.digest
                end

                it "selects it by a partial digest" do
                    @context.dataset_select @dataset.digest[0, 5]
                    assert_equal @dataset.digest, @context.dataset.digest
                end

                it "selects it by metadata" do
                    @dataset.metadata_set "key", "value"
                    @dataset.metadata_write_to_file
                    @context.dataset_select "key" => "value"
                    assert_equal @dataset.digest, @context.dataset.digest
                end

                it "runs an interactive picker if more than one dataset matches "\
                   "the metadata" do
                    @dataset.metadata_set "key", "value"
                    @dataset.metadata_write_to_file

                    other = create_dataset("something") {}
                    other.metadata_set "key", "value"
                    other.metadata_write_to_file

                    expected_paths = [@dataset, other].map(&:dataset_path).to_set
                    actual_paths = nil
                    flexmock(@context)
                        .should_receive(:__dataset_user_select)
                        .with(->(sets) { actual_paths = sets.map(&:dataset_path) })
                        .and_return(other)

                    @context.dataset_select "key" => "value"
                    assert_equal expected_paths, actual_paths.to_set
                    assert_equal other.digest, @context.dataset.digest
                end

                it "raises if no datasets matches the digest" do
                    assert_raises(ArgumentError) do
                        @context.dataset_select "bla"
                    end
                end

                it "raises if no datasets matches the metadata" do
                    assert_raises(ArgumentError) do
                        @context.dataset_select "bla" => "blo"
                    end
                end
            end

            describe "#samples_of" do
                attr_reader :now

                before do
                    now_nsec = Time.now
                    @now = Time.at(now_nsec.tv_sec, now_nsec.tv_usec)
                    create_dataset "exists" do
                        create_logfile "test.0.log" do
                            create_logfile_stream(
                                "test", metadata: {
                                    "rock_task_name" => "task_test",
                                    "rock_task_object_name" => "port_test",
                                    "rock_stream_type" => "port"
                                }
                            )
                            write_logfile_sample now, now, 10
                            write_logfile_sample now + 10, now + 1, 20
                        end
                    end
                end

                it "returns a sample enumerator as-is" do
                    @context = make_context
                    @context.datastore_select @datastore_path
                    @context.dataset_select
                    samples = @context.task_test_task.port_test_port.samples
                    assert_same samples, @context.samples_of(samples)
                end

                it "returns a stream's sample enumerator" do
                    @context = make_context
                    @context.datastore_select @datastore_path
                    @context.dataset_select
                    port = @context.task_test_task.port_test_port
                    samples = @context.samples_of(port)
                    expected = [
                        [now, now, 10],
                        [now + 10, now + 1, 20]
                    ]
                    assert_equal expected, samples.enum_for(:each).to_a
                end

                it "restricts the created enumerator to the defined interval "\
                   "if there is one" do
                    @context = make_context
                    @context.datastore_select @datastore_path
                    @context.dataset_select

                    port = @context.task_test_task.port_test_port
                    @context.interval_select_from_stream(port)
                    @context.interval_shift_start(0.1)
                    #@context.interval_shift_end(0.1)
                    samples = @context.samples_of(port)
                    expected = [
                        [now + 10, now + 1, 20]
                    ]
                    assert_equal expected, samples.enum_for(:each).to_a
                end
            end

            describe "#realign" do
                it "raises if the first target time is earlier than the dataframe's" do
                    time  = [10, 11, 12, 13, 14]
                    vector = [1.1, 2.2, 3.3, 4.4, 5.5]
                    df = ::Daru::DataFrame.new({ "time" => time, "data" => vector })

                    target = [9, 10.1, 10.9]
                    assert_raises(ArgumentError) { make_context.realign(target, df) }
                end
                it "raises if the last target time is later than the dataframe's" do
                    time  = [10, 11, 12, 13, 14]
                    vector = [1.1, 2.2, 3.3, 4.4, 5.5]
                    df = ::Daru::DataFrame.new({ "time" => time, "data" => vector })

                    target = [13.1, 13.9, 15]
                    assert_raises(ArgumentError) { make_context.realign(target, df) }
                end

                it "picks the sample whose time is closest to the target time" do
                    time  = [10, 11, 12, 13, 14]
                    vector = [1.1, 2.2, 3.3, 4.4, 5.5]
                    df = ::Daru::DataFrame.new({ "time" => time, "data" => vector })

                    target = [10.1, 13.3, 13.9]
                    result = make_context.realign(target, df)
                    assert_equal target, result["time"].to_a
                    assert_equal [1.1, 4.4, 5.5], result["data"].to_a
                end
            end

            def make_context
                context = Object.new
                context.extend DSL
                context
            end
        end
    end
end
