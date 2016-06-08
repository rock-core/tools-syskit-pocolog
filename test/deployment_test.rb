require 'test_helper'

module Syskit::Pocolog
    describe Deployment do
        attr_reader :replay_manager
        attr_reader :replay_manager, :streams, :port_stream, :task_m, :deployment_m
        attr_reader :subject
        before do
            double_t = Roby.app.default_loader.registry.get '/double'

            create_log_file 'test'
            stream0 = create_log_stream '/port0', double_t,
                'rock_task_name' => "task",
                'rock_task_object_name' => 'out',
                'rock_stream_type' => 'port'
            stream1 = create_log_stream '/port1', double_t,
                'rock_task_name' => "task",
                'rock_task_object_name' => 'other_out',
                'rock_stream_type' => 'port'
            stream0.write Time.at(0), Time.at(0), 0
            stream1.write Time.at(1), Time.at(1), 1
            flush_log_file
            @streams = Streams.from_dir(created_log_dir).
                find_task_by_name('task')
            @port_stream = streams.find_port_by_name('out')
            @task_m = Syskit::TaskContext.new_submodel do
                output_port 'out', double_t
            end
            @replay_manager = execution_engine.pocolog_replay_manager

            @deployment_m = Syskit::Pocolog::Deployment.new_submodel(task_model: task_m, task_name: 'task')
            deployment_m.add_streams_from streams
            plan.add_permanent_task(@subject = deployment_m.new(process_name: 'test', on: 'pocolog'))
        end

        it "gets notified of new samples when running" do
            subject.start!
            flexmock(subject).should_receive(:process_sample).
                with(port_stream, Time.at(0), 0).
                once
            replay_manager.step
        end

        it "does nothing if the streams are eof?" do
            subject.start!
            replay_manager.step
            replay_manager.step
        end

        describe "dynamic stream addition and removal" do
            attr_reader :other_deployment
            before do
                subject.start!
                replay_manager.step

                other_task_m = Syskit::TaskContext.new_submodel do
                    output_port 'other_out', '/double'
                end
                other_deployment_m = Syskit::Pocolog::Deployment.new_submodel(task_model: other_task_m, task_name: 'task')
                other_deployment_m.add_streams_from streams
                @other_deployment = other_deployment_m.new(process_name: 'other_test', on: 'pocolog')
                plan.add_permanent_task(other_deployment)
            end

            it "does not skip a sample when eof? and a new stream is added to the alignment" do
                replay_manager.step

                other_deployment.start!
                flexmock(other_deployment).should_receive(:process_sample).
                    with(streams.find_port_by_name('other_out'), Time.at(1), 1).
                    once
                replay_manager.step
            end

            it "does not skip a sample when the current sample is from a stream that has been removed" do
                other_deployment.start!
                subject.stop!
                assert_event_emission subject.stop_event
                flexmock(other_deployment).should_receive(:process_sample).
                    with(streams.find_port_by_name('other_out'), Time.at(1), 1).
                    once
                replay_manager.step
            end
        end

        it "does not get notified if pending" do
            flexmock(subject).should_receive(:process_sample).never
            replay_manager.step
        end

        it "does not get notified if stopped" do
            subject.start!
            subject.stop!
            assert_event_emission(subject.stop_event)
            flexmock(subject).should_receive(:process_sample).never
            replay_manager.step
        end
        
        it "forwards the samples to an existing, running, deployed task" do
            plan.add_permanent_task(task = subject.task('task'))
            syskit_configure_and_start(task)
            reader = task.orocos_task.out.reader
            subject.process_sample(port_stream, Time.now, 1)
            sample = assert_has_one_new_sample reader
            assert_equal 1, sample
        end
        
        it "does not forward the samples to a configured task" do
            plan.add_permanent_task(task = subject.task('task'))
            syskit_configure(task)
            flexmock(task.orocos_task.out).should_receive(:write).never
            subject.process_sample(port_stream, Time.now, 1)
        end
        it "does not forward the samples to a finished task" do
            plan.add_permanent_task(task = subject.task('task'))
            syskit_configure_and_start(task)
            plan.unmark_permanent_task(task)
            task.stop!
            assert_event_emission(task.stop_event)

            flexmock(task.orocos_task.out).should_receive(:write).never
            subject.process_sample(port_stream, Time.now, 1)
        end
    end
end
