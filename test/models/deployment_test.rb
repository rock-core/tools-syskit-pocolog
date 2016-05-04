require 'test_helper'

module Syskit::Pocolog
    module Models
        describe Deployment do
            attr_reader :streams
            before do
                double_t = Roby.app.default_loader.registry.get '/double'

                create_log_file 'test'
                create_log_stream '/port0', double_t,
                    'rock_task_name' => "task",
                    'rock_task_object_name' => 'object0',
                    'rock_stream_type' => 'port'
                create_log_stream '/port1', '/double',
                    'rock_task_name' => "task_with_mismatching_type",
                    'rock_task_object_name' => 'port_with_mismatching_type',
                    'rock_stream_type' => 'port'
                flush_log_file
                @streams = Streams.from_dir(created_log_dir).
                    find_task_by_name('task')
            end

            describe "#add_stream" do
                describe "error cases" do
                    attr_reader :deployment_m, :task_m
                    before do
                        @task_m = task_m = Syskit::TaskContext.new_submodel do
                            input_port 'in', '/double'
                            output_port 'out', '/double'
                        end
                        @deployment_m = Syskit::Pocolog::Deployment.new_submodel(task_model: task_m, task_name: 'task')
                    end

                    it "raises ArgumentError if the port is not a port of the deployment's task model" do
                        other_task_m = Syskit::TaskContext.new_submodel do
                            output_port 'out', '/double'
                        end
                        assert_raises(ArgumentError) do
                            deployment_m.add_stream(streams.find_task_by_name('object0'), other_task_m.out_port)
                        end
                    end
                    it "raises MismatchingType if the stream and port have different types" do
                        streams = Streams.from_dir(created_log_dir).
                            find_task_by_name('task_with_mismatching_type')
                        replay_task_m = Syskit::Pocolog::ReplayTaskContext.model_for(task_m.orogen_model)
                        assert_raises(MismatchingType) do
                            deployment_m.add_stream(streams.find_port_by_name('port_with_mismatching_type'), replay_task_m.out_port)
                        end
                    end
                    it "raises ArgumentError if the port is an input port" do
                        assert_raises(ArgumentError) do
                            deployment_m.add_stream(streams.find_task_by_name('object0'), task_m.in_port)
                        end
                    end
                end

                it "uses the task's port with the same name by default" do
                    task_m = Syskit::TaskContext.new_submodel do
                        output_port 'object0', '/double'
                    end
                    replay_task_m = Syskit::Pocolog::ReplayTaskContext.model_for(task_m.orogen_model)
                    deployment_m = Syskit::Pocolog::Deployment.new_submodel(task_model: task_m, task_name: 'task')
                    deployment_m.add_stream(port_stream = streams.find_port_by_name('object0'))
                    assert_equal Hash[port_stream => replay_task_m.object0_port],
                        deployment_m.streams_to_port
                end
            end

            describe "#add_streams_from" do
                it "issues a warning if allow_missing is true and some output ports do not have a matching stream" do
                    task_m = Syskit::TaskContext.new_submodel do
                        output_port 'unknown_port', '/double'
                    end
                    deployment_m = Syskit::Pocolog::Deployment.new_submodel(task_name: 'test', task_model: task_m)
                    flexmock(Syskit::Pocolog, :strict).should_receive(:warn).with(/unknown_port/).once
                    deployment_m.add_streams_from(streams, allow_missing: true)
                end

                it "raises MissingStream if allow_missing is false and some output ports do not have a matching stream" do
                    task_m = Syskit::TaskContext.new_submodel do
                        output_port 'unknown_port', '/double'
                    end
                    deployment_m = Syskit::Pocolog::Deployment.new_submodel(task_name: 'test', task_model: task_m)
                    assert_raises(MissingStream) do
                        deployment_m.add_streams_from(streams, allow_missing: false)
                    end
                end

                it "adds the matching streams" do
                    task_m = Syskit::TaskContext.new_submodel do
                        output_port 'object0', '/double'
                    end
                    replay_task_m = Syskit::Pocolog::ReplayTaskContext.model_for(task_m.orogen_model)
                    deployment_m = Syskit::Pocolog::Deployment.new_submodel(task_name: 'test', task_model: task_m)
                    flexmock(deployment_m).should_receive(:add_stream).
                        with(streams.find_port_by_name('object0'), replay_task_m.object0_port).
                        once
                    deployment_m.add_streams_from(streams)
                end
            end
        end
    end
end

