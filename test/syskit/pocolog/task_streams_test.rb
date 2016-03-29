require 'test_helper'

module Syskit
    module Pocolog
        describe TaskStreams do
            attr_reader :subject
            before do
                create_log_file 'test'
                create_log_stream '/port0', '/double',
                    'rock_task_name' => "task",
                    'rock_task_object_name' => 'object0',
                    'rock_stream_type' => 'port'
                create_log_stream '/port1_1', '/double',
                    'rock_task_name' => "task",
                    'rock_task_object_name' => 'object1',
                    'rock_stream_type' => 'port'
                create_log_stream '/port1_2', '/double',
                    'rock_task_name' => "task",
                    'rock_task_object_name' => 'object1',
                    'rock_stream_type' => 'port'
                create_log_stream '/property0', '/double',
                    'rock_task_name' => "task",
                    'rock_task_object_name' => 'object0',
                    'rock_stream_type' => 'property'
                create_log_stream '/property1_1', '/double',
                    'rock_task_name' => "task",
                    'rock_task_object_name' => 'object1',
                    'rock_stream_type' => 'property'
                create_log_stream '/property1_2', '/double',
                    'rock_task_name' => "task",
                    'rock_task_object_name' => 'object1',
                    'rock_stream_type' => 'property'
                flush_log_file
                streams = Streams.from_dir(created_log_dir)
                @subject = streams.find_task_by_name('task')
            end

            describe "#find_port_by_name" do
                it "returns nil if there are no matches" do
                    assert !subject.find_port_by_name('does_not_exist')
                end
                it "returns the matching port stream" do
                    object = subject.find_port_by_name('object0')
                    assert_kind_of ::Pocolog::DataStream, object
                    assert_equal '/port0', object.name
                end
                it "raises Ambiguous if there are more than one port with the given name" do
                    assert_raises(Ambiguous) do
                        subject.find_port_by_name('object1')
                    end
                end

                describe "access through #method_missing" do
                    it "returns a single match if there is one" do
                        assert_equal '/port0', subject.object0_port.name
                    end
                    it "raises Ambiguous for multiple matches" do
                        assert_raises(Ambiguous) do
                            subject.object1_port
                        end
                    end
                    it "raises NoMethodError if there are no matches" do
                        assert_raises(NoMethodError) do
                            subject.does_not_exist_port
                        end
                    end
                end
            end

            describe "#find_property_by_name" do
                it "returns nil if there are no matches" do
                    assert !subject.find_property_by_name('does_not_exist')
                end
                it "returns the matching port stream" do
                    object = subject.find_property_by_name('object0')
                    assert_kind_of ::Pocolog::DataStream, object
                    assert_equal '/property0', object.name
                end
                it "raises Ambiguous if there are more than one port with the given name" do
                    assert_raises(Ambiguous) do
                        subject.find_property_by_name('object1')
                    end
                end

                describe "access through #method_missing" do
                    it "returns a single match if there is one" do
                        assert_equal '/property0', subject.object0_property.name
                    end
                    it "raises Ambiguous for multiple matches" do
                        assert_raises(Ambiguous) do
                            subject.object1_property
                        end
                    end
                    it "raises NoMethodError if there are no matches" do
                        assert_raises(NoMethodError) do
                            subject.does_not_exist_property
                        end
                    end
                end
            end
        end
    end
end

