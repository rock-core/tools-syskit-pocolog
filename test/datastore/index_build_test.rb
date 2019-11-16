require 'test_helper'
require 'roby/test/droby_test_helpers'
require 'syskit/pocolog/datastore/index_build'

module Syskit::Pocolog
    class Datastore
        describe IndexBuild do
            attr_reader :datastore, :dataset, :index_build
            before do
                @datastore = Datastore.create(logfile_pathname('datastore'))
                @dataset = create_dataset 'TEST'
                @index_build = IndexBuild.new(datastore, dataset)
            end
            def dataset_path
                dataset.dataset_path
            end
            def cache_path
                dataset.cache_path
            end

            describe "#rebuild_pocolog_indexes" do
                before do
                    create_logfile 'task::port.0.log' do
                        create_logfile_stream 'test', metadata: Hash['rock_task_name' => 'task',
                                                                   'rock_task_object_name' => 'port']
                        write_logfile_sample Time.now, Time.now, 10
                    end
                end

                it "does nothing if the dataset has no pocolog streams" do
                    logfile_pathname('task::port.0.log').unlink
                    index_build.rebuild_pocolog_indexes
                end
                it "creates the cache dir if it is missing" do
                    index_build.rebuild_pocolog_indexes
                    assert (dataset.cache_path + "pocolog").directory?
                end
                it "does nothing if a valid index file exists" do
                    pocolog_index_dir = (dataset.cache_path + "pocolog")
                    open_logfile('task::port.0.log', index_dir: pocolog_index_dir).close
                    index_contents = (pocolog_index_dir + "task::port.0.idx").read
                    flexmock(Pocolog::Format::Current).
                        should_receive(:rebuild_index_file).
                        never

                    index_build.rebuild_pocolog_indexes
                    assert_equal index_contents, (dataset.cache_path + "pocolog" + "task::port.0.idx").read
                end
                it "rebuilds the index if the file is not valid" do
                    pocolog_index_dir = (dataset.cache_path + "pocolog")
                    open_logfile('task::port.0.log', index_dir: pocolog_index_dir).close
                    flexmock(Pocolog::Format::Current).
                        should_receive(:read_index_stream_info).once.
                        and_raise(Pocolog::InvalidIndex)
                    flexmock(Pocolog::Format::Current).
                        should_receive(:rebuild_index_file).
                        once.pass_thru

                    index_build.rebuild_pocolog_indexes
                end
                it "forces index rebuilding if 'force' is true" do
                    pocolog_index_dir = (dataset.cache_path + "pocolog")
                    open_logfile('task::port.0.log', index_dir: pocolog_index_dir).close
                    index_contents = (pocolog_index_dir + "task::port.0.idx").read
                    flexmock(Pocolog::Format::Current).
                        should_receive(:rebuild_index_file).
                        once.pass_thru

                    index_build.rebuild_pocolog_indexes(force: true)
                    assert_equal index_contents, (dataset.cache_path + "pocolog" + "task::port.0.idx").read
                end
                it "creates a new index file if none exists" do
                    flexmock(Pocolog::Format::Current).
                        should_receive(:rebuild_index_file).
                        once.pass_thru
                    index_build.rebuild_pocolog_indexes(force: true)
                    assert (dataset.cache_path + "pocolog" + "task::port.0.idx").exist?
                end
            end

            describe "#rebuild_roby_index" do
                include Roby::Test::DRobyTestHelpers
                before do
                    droby_create_event_log(dataset_path + 'roby-events.log') do
                        droby_write_event :test, 10
                    end
                end

                it "does nothing if there are no roby indexes" do
                    (dataset_path + "roby-events.log").unlink
                    index_build.rebuild_roby_index
                end

                it "creates the cache dir if it is missing" do
                    index_build.rebuild_roby_index
                    assert cache_path.directory?
                end
                it "does nothing if a valid index file exists" do
                    cache_path.mkpath
                    Roby::DRoby::Logfile::Index.rebuild_file(
                        dataset_path + "roby-events.log",
                        cache_path + "roby-events.idx")
                    flexmock(Roby::DRoby::Logfile::Index).should_receive(:rebuild).never
                    index_build.rebuild_roby_index
                end
                it "rebuilds if a valid index file exists but force is true" do
                    cache_path.mkpath
                    Roby::DRoby::Logfile::Index.rebuild_file(
                        dataset_path + "roby-events.log",
                        cache_path + "roby-events.idx")
                    flexmock(Roby::DRoby::Logfile::Index).
                        should_receive(:rebuild).once.pass_thru
                    index_build.rebuild_roby_index(force: true)
                end
                it "rebuilds if no index file exists" do
                    flexmock(Roby::DRoby::Logfile::Index).
                        should_receive(:rebuild).
                        once.pass_thru
                    index_build.rebuild_roby_index
                    assert Roby::DRoby::Logfile::Index.valid_file?(
                        dataset_path + "roby-events.log",
                        cache_path + "roby-events.idx")
                end
                it "skips the roby file if its format is not current" do
                    (dataset_path + "roby-events.log").open('w') do |io|
                        Roby::DRoby::Logfile.write_header(io, version: 0)
                    end
                    reporter = flexmock(Pocolog::CLI::NullReporter.new)
                    reporter.should_receive(:warn).once.
                        with(/roby-events.log is an obsolete Roby log file format, skipping/)

                    index_build.rebuild_roby_index(reporter: reporter)
                end
            end
        end
    end
end

