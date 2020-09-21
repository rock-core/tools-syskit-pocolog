# frozen_string_literal: true

require "test_helper"

module Syskit
    module Log
        module RobySQLIndex
            describe Accessors do
                before do
                    @index = Index.create(logfile_pathname("roby.sql"))
                    @index.add_roby_log(roby_log_path("accessors"))
                    @root = Accessors::Root.new(@index)
                end

                describe "the root" do
                    it "raises for a name that is not a known namespace" do
                        e = assert_raises(NoMethodError) do
                            @root.does_not_exist
                        end
                        assert_equal :does_not_exist, e.name
                    end

                    it "gives access to a namespace" do
                        assert @root.Namespace
                    end
                end

                describe "a namespace" do
                    before do
                        @namespace = @root.Namespace
                    end

                    it "raises for a name that is not a known namespace" do
                        e = assert_raises(NoMethodError) do
                            @namespace.does_not_exist
                        end
                        assert_equal :does_not_exist, e.name
                    end

                    it "gives access to a task model" do
                        assert @namespace.M
                    end
                end

                describe "a task model" do
                    before do
                        @task_model = @root.Namespace.M
                    end

                    it "raises for a name that is not a known namespace" do
                        e = assert_raises(NoMethodError) do
                            @task_model.does_not_exist
                        end
                        assert_equal :does_not_exist, e.name
                    end

                    it "gives access to a task model" do
                        assert @task_model.Submodel
                    end

                    it "raises if trying to access an event that does not exist" do
                        e = assert_raises(NoMethodError) do
                            @task_model.not_an_event
                        end
                        assert_equal :not_an_event, e.name, e.message
                    end

                    it "gives access to an existing event" do
                        assert @task_model.start_event
                    end

                    it "allows to enumerate all events of the task model" do
                        assert_equal %w[start failed stop],
                                     @task_model.each_event.map(&:name)
                    end
                end

                describe "an event" do
                    before do
                        @event_model = @root.Namespace.M.start_event
                    end

                    it "allows to enumerate the event's emissions" do
                        emissions = @event_model.each_emission.to_a
                        assert_equal 1, emissions.size
                        assert_kind_of Entities::EmittedEvent, emissions.first
                    end
                end
            end
        end
    end
end
