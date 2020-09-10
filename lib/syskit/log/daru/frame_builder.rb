# frozen_string_literal: true

module Syskit
    module Log
        module Daru
            # Utility class that allows to create a Daru dataframe based on
            # fields from a pocolog stream
            class FrameBuilder
                attr_reader :time_field
                attr_reader :vector_fields

                def initialize(type)
                    @type = type
                    @time_field = nil
                    @vector_fields = []

                    guess_time_field if @type <= Typelib::CompoundType
                end

                def guess_time_field
                    time_field = @type
                                 .each_field
                                 .find { |_, field_type| field_type.name == "/base/Time" }
                    return unless time_field

                    time { |b| b.__send__(time_field[0]).microseconds }
                end

                def time(&block)
                    @time_field = resolve_field(&block)
                end

                def add(name = nil, &block)
                    resolved = resolve_field(&block)
                    resolved.name = name if name
                    @vector_fields << resolved
                end

                ResolvedField = Struct.new :name, :path, :transform do
                    def resolve(value)
                        v = path.resolve(value).first.to_ruby
                        transform ? transform.call(v) : v
                    end
                end

                class InvalidDataType < ArgumentError; end

                def resolve_field
                    builder = yield(PathBuilder.new(@type))
                    unless builder.__terminal?
                        raise InvalidDataType,
                              "field resolved to type #{builder.__type}, "\
                              "which is not simple nor transformed"
                    end
                    ResolvedField.new(builder.__name, builder.__path, builder.__transform)
                end

                def to_daru_frame(start_time, samples)
                    if @vector_fields.empty?
                        raise ArgumentError, "no vector fields defined with #add"
                    end

                    if @time_field
                        to_daru_frame_with_time(start_time, samples)
                    else
                        to_daru_frame_without_time(start_time, samples)
                    end
                end

                def to_daru_frame_with_time(start_time, samples)
                    data = [[@time_field, []]] + @vector_fields.map { |p| [p, []] }
                    samples.raw_each do |_, _, sample|
                        data.each do |field, path_data|
                            path_data << field.resolve(sample)
                        end
                    end

                    time = data.shift[1]
                    start_time_us = start_time.tv_sec * 1_000_000 + start_time.tv_usec
                    time.map! { |v| (v - start_time_us) / 1_000_000.0 }

                    create_daru_frame(time, data)
                end

                def to_daru_frame_without_time(start_time, samples)
                    time = []
                    data = @vector_fields.map { |p| [p, []] }
                    samples.raw_each do |_, lg, sample|
                        time << lg - start_time
                        data.each do |field, path_data|
                            path_data << field.resolve(sample)
                        end
                    end

                    create_daru_frame(time, data)
                end

                def create_daru_frame(time, vectors)
                    vectors = vectors.each_with_object({}) do |(field, path_data), h|
                        h[field.name] = path_data
                    end

                    ::Daru::DataFrame.new(vectors, index: time, dtype: :gsl)
                end
            end
        end
    end
end
