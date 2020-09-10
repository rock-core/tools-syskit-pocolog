# frozen_string_literal: true

require "test_helper"
require "syskit/log/daru"

module Syskit
    module Log
        module Daru # :nodoc:
            describe FrameBuilder do
                before do
                    @registry = Typelib::Registry.new
                    @uint64_t = @registry.create_numeric "/uint64_t", 8, :uint
                    @base_time_t = @registry.create_compound "/base/Time" do |c|
                        c.microseconds = "/uint64_t"
                    end
                end

                it "guesses the time field if the type is a compound and "\
                   "has a field of type /base/Time" do
                    compound_t = @registry.create_compound "/C" do |c|
                        c.time = "/base/Time"
                    end

                    builder = FrameBuilder.new(compound_t)
                    assert_equal ".time.microseconds", builder.time_field.name
                end

                it "silently ignores if the compound has no time fields" do
                    compound_t = @registry.create_compound "/C" do |c|
                        c.time = "/uint64_t"
                    end

                    builder = FrameBuilder.new(compound_t)
                    assert_nil builder.time_field
                end

                it "does not attempt to guess a time field for an array" do
                    array_t = @registry.create_array @base_time_t, 5

                    builder = FrameBuilder.new(array_t)
                    assert_nil builder.time_field
                end

                it "applies the transform block if one is defined" do
                    compound_t = @registry.create_compound "/C" do |c|
                        c.time = "/base/Time"
                        c.value = "/uint64_t"
                    end

                    samples = mock_samples do |i|
                        compound_t.new(
                            time: { microseconds: i * 1_000_000 + 3 },
                            value: i * 2
                        )
                    end

                    builder = FrameBuilder.new(compound_t)
                    builder.add { |b| b.value.transform { |i| i * 2 } }

                    start_time = Time.at(0, 3)
                    frame = builder.to_daru_frame(start_time, samples)

                    assert_equal (1..10).to_a, frame.index.to_a
                    assert_equal (4..40).step(4).to_a, frame[".value"].to_a
                end

                it "builds a frame using a defined time field" do
                    compound_t = @registry.create_compound "/C" do |c|
                        c.time = "/base/Time"
                        c.value = "/uint64_t"
                    end

                    samples = mock_samples do |i|
                        compound_t.new(
                            time: { microseconds: i * 1_000_000 + 3 },
                            value: i * 2
                        )
                    end

                    builder = FrameBuilder.new(compound_t)
                    builder.add(&:value)

                    start_time = Time.at(0, 3)
                    frame = builder.to_daru_frame(start_time, samples)

                    assert_equal (1..10).to_a, frame.index.to_a
                    assert_equal (2..20).step(2).to_a, frame[".value"].to_a
                end

                it "uses the sample's logical time if no time field is selected" do
                    compound_t = @registry.create_compound "/C" do |c|
                        c.value = "/uint64_t"
                    end

                    samples = mock_samples do |i|
                        compound_t.new(value: i * 2)
                    end

                    builder = FrameBuilder.new(compound_t)
                    builder.add(&:value)

                    start_time = Time.at(0, 2)
                    frame = builder.to_daru_frame(start_time, samples)

                    assert_equal (1..10).to_a, frame.index.to_a
                    assert_equal (2..20).step(2).to_a, frame[".value"].to_a
                end

                def mock_samples
                    samples = flexmock
                    iterations = (1..10).map do |i|
                        rt = Time.at(i, 1)
                        lg = Time.at(i, 2)
                        [rt, lg, yield(i)]
                    end

                    samples.should_receive(:raw_each)
                           .and_iterates(*iterations)
                    samples
                end
            end
        end
    end
end
