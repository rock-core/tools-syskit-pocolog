# frozen_string_literal: true

module Syskit
    module Log
        module RobySQLIndex
            module Accessors
                # Represents the query root
                #
                # It gives access to the constants under it through the method
                # missing interface
                class Root
                    def initialize(index)
                        @index = index
                        @prefix = ""
                        @separator = "::"
                        @namespace_class = Namespace
                    end

                    def validate_method_missing_noargs(m, args, kw)
                        return if args.empty? && kw.empty?

                        raise ArgumentError,
                              "#{m} expected zero arguments, but got #{args.size} "\
                              "positional and #{kw.size} keyword arguments"
                    end

                    def respond_to_missing?(m, _include_private = false)
                        full_name = "#{@prefix}#{m}"
                        @index.models.where(name: full_name).exist? ||
                            @index.models.where { name.like("#{full_name}::%") }.exist? ||
                            super
                    end

                    def method_missing(m, *args, **kw, &block)
                        full_name = "#{@prefix}#{m}"
                        pattern = "#{@prefix}#{m}#{@separator}"
                        if m == :OroGen
                            OroGenNamespace.new(@index, "OroGen")
                        elsif @index.models.where(name: full_name).exist?
                            validate_method_missing_noargs(m, args, kw)
                            TaskModel.new(@index, full_name)
                        elsif @index.models.where { name.like("#{pattern}%") }.exist?
                            validate_method_missing_noargs(m, args, kw)
                            @namespace_class.new(@index, full_name)
                        else
                            super
                        end
                    end
                end

                # The OroGen model hierarchy
                class OroGenNamespace < Root
                    def initialize(index, name)
                        super(index)
                        @prefix = "#{name}."
                        @separator = "."
                        @namespace_class = OroGenNamespace
                    end
                end

                # A non-root namespace
                #
                # It gives access to the constants under it through the method
                # missing interface
                class Namespace < Root
                    def initialize(index, name)
                        super(index)
                        @prefix = "#{name}::"
                    end
                end

                # A task model
                #
                # It can give access to the constants under it, or to the events
                # that are known to the index
                class TaskModel < Namespace
                    def initialize(index, name)
                        super(index, name)
                        @name = name
                        model = @index.models.where(name: name).one!
                        @query = @index.tasks.where(model_id: model.id)
                    end

                    def each_event
                        return enum_for(__method__) unless block_given?

                        @index.history_of(@query)
                              .select(:name).distinct
                              .pluck(:name).each do |event_name|
                                  yield(EventModel.new(@index, event_name, @query))
                              end
                    end

                    def method_missing(m, *args, **kw, &block)
                        m_to_s = m.to_s
                        return super unless m_to_s.end_with?("_event")

                        event_name = m_to_s.gsub(/_event$/, "")
                        unless @index.event_with_name?(event_name)
                            raise NoMethodError.new(m),
                                  "no events named #{event_name} have been emitted"
                        end

                        has_events =
                            @index.history_of(@index.tasks_by_model_name(@name))
                                  .where(name: event_name)
                                  .exist?

                        unless has_events
                            raise NoMethodError.new(m),
                                  "there are emitted events named #{event_name}, but "\
                                  "not for a task of model #{@name}"
                        end

                        EventModel.new(@index, event_name, @query)
                    end
                end

                # Represents an event generator model
                class EventModel
                    attr_reader :name

                    def initialize(index, name, task_query)
                        @index = index
                        @name = name
                        @query = @index.emitted_events
                                       .where(name: name, task_id: task_query.pluck(:id))
                    end

                    # List the matching event emissions
                    def each_emission(&block)
                        @query.combine(task: :model).each(&block)
                    end

                    # Get the first emission
                    def first_emission
                        each_emission.first
                    end
                end
            end
        end
    end
end
