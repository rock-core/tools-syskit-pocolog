require 'test_helper'
require 'syskit/pocolog/normalize'

module Syskit::Pocolog
    describe Normalize do
        attr_reader :normalize, :base_time
        before do
            @base_time = Time.new(1980, 9, 30)
            @normalize = Normalize.new
        end

        describe "#normalize" do
            before do
                create_logfile 'file0.0.log' do
                    create_logfile_stream 'stream0', metadata: Hash['rock_task_name' => 'task0', 'rock_task_object_name' => 'port']
                    write_logfile_sample base_time + 2, base_time + 20, 2
                    create_logfile_stream 'stream1', metadata: Hash['rock_task_name' => 'task1', 'rock_task_object_name' => 'port']
                    write_logfile_sample base_time + 1, base_time + 10, 1
                end
            end

            it "splits the file into a one-file-per-stream scheme" do
                logfile_pathname('normalized').mkdir
                normalize.normalize([logfile_pathname('file0.0.log')])
                normalized_dir = logfile_pathname('normalized')
                stream = open_logfile_stream (normalized_dir + "task0::port.0.log"), 'stream0'
                assert_equal [[base_time + 2, base_time + 20, 2]], stream.samples.to_a
                stream = open_logfile_stream (normalized_dir + "task1::port.0.log"), 'stream1'
                assert_equal [[base_time + 1, base_time + 10, 1]], stream.samples.to_a
            end
            it "optionally computes the sha256 digest of the generated file, without the prologue" do
                logfile_pathname('normalized').mkdir
                result = normalize.normalize([logfile_pathname('file0.0.log')], compute_sha256: true)

                path = logfile_pathname('normalized', 'task0::port.0.log')
                expected = Digest::SHA256.hexdigest(path.read[Pocolog::Format::Current::PROLOGUE_SIZE..-1])
                assert_equal expected, result[path].hexdigest
            end
            it "generates valid index files for the normalized streams" do
                logfile_pathname('normalized').mkdir
                normalize.normalize([logfile_pathname('file0.0.log')])
                flexmock(Pocolog::Logfiles).new_instances.
                    should_receive(:rebuild_and_load_index).
                    never
                normalized_dir = logfile_pathname('normalized')
                open_logfile_stream (normalized_dir + "task0::port.0.log"), 'stream0'
                open_logfile_stream (normalized_dir + "task1::port.0.log"), 'stream1'
            end
            it "detects followup streams" do
                create_logfile 'file0.1.log' do
                    create_logfile_stream 'stream0', metadata: Hash['rock_task_name' => 'task0', 'rock_task_object_name' => 'port']
                    write_logfile_sample base_time + 3, base_time + 30, 3
                end
                normalize.normalize([logfile_pathname('file0.0.log'), logfile_pathname('file0.1.log')])
                normalized_dir = logfile_pathname('normalized')
                stream = open_logfile_stream (normalized_dir + "task0::port.0.log"), 'stream0'
                assert_equal [[base_time + 2, base_time + 20, 2],
                              [base_time + 3, base_time + 30, 3]], stream.samples.to_a
            end
            it "raises if a potential followup stream has an non-matching realtime range" do
                create_logfile 'file0.1.log' do
                    create_logfile_stream 'stream0', metadata: Hash['rock_task_name' => 'task0', 'rock_task_object_name' => 'port']
                    write_logfile_sample base_time + 1, base_time + 30, 3
                end
                capture_io do
                    assert_raises(Normalize::InvalidFollowupStream) do
                        normalize.normalize([logfile_pathname('file0.0.log'), logfile_pathname('file0.1.log')])
                    end
                end
            end
            it "raises if a potential followup stream has an non-matching logical time range" do
                create_logfile 'file0.1.log' do
                    create_logfile_stream 'stream0', metadata: Hash['rock_task_name' => 'task0', 'rock_task_object_name' => 'port']
                    write_logfile_sample base_time + 3, base_time + 10, 3
                end
                capture_io do
                    assert_raises(Normalize::InvalidFollowupStream) do
                        normalize.normalize([logfile_pathname('file0.0.log'), logfile_pathname('file0.1.log')])
                    end
                end
            end
            it "raises if a potential followup stream has an non-matching type" do
                create_logfile 'file0.1.log' do
                    stream_t = Typelib::Registry.new.create_numeric '/test_t', 8, :sint
                    create_logfile_stream 'stream0',
                        type: stream_t,
                        metadata: Hash['rock_task_name' => 'task0', 'rock_task_object_name' => 'port']
                    write_logfile_sample base_time + 3, base_time + 30, 3
                end
                capture_io do
                    assert_raises(Normalize::InvalidFollowupStream) do
                        normalize.normalize([logfile_pathname('file0.0.log'), logfile_pathname('file0.1.log')])
                    end
                end
            end
            it "deletes newly created files if the initialization of a new file fails" do
                create_logfile 'file0.1.log' do
                    create_logfile_stream 'stream0',
                        metadata: Hash['rock_task_name' => 'task0', 'rock_task_object_name' => 'port']
                    write_logfile_sample base_time + 3, base_time + 30, 3
                end
                error_class = Class.new(Exception)
                flexmock(File).new_instances.should_receive(:write).and_raise(error_class)
                _out, err = capture_io do
                    assert_raises(error_class) do
                        normalize.normalize([logfile_pathname('file0.0.log'), logfile_pathname('file0.1.log')])
                    end
                end
                normalized_dir = logfile_pathname('normalized')
                refute (normalized_dir + "task0::port.0.log").exist?
            end
        end
    end
end

