require 'roby'
require 'optparse'
require 'syskit'

app = Roby.app
app.require_app_dir
app.public_shell_interface = true
app.public_logs = true
app.using 'syskit'
app.using 'syskit-pocolog'

MetaRuby.keep_definition_location = false

options = OptionParser.new do |opt|
    opt.banner = <<-EOD
syskit pocolog [-r ROBOT] /path/to/dataset
    EOD
    Roby::Application.common_optparse_setup(opt)
end
remaining = options.parse(ARGV)

if remaining.empty?
    Syskit::Pocolog.fatal "dataset directory missing"
    puts options
    exit 1
elsif remaining.size > 1
    Syskit::Pocolog.fatal "too many arguments"
    puts options
    exit 1
end

dataset_dir = Pathname.new(remaining.first)
if !dataset_dir.exist?
    Syskit::Pocolog.fatal "#{dataset_dir} does not exist"
    exit 1
elsif !dataset_dir.directory?
    Syskit::Pocolog.fatal "#{dataset_dir} is not a directory"
    exit 1
end

Roby.display_exception do
    app.setup
    begin
        streams = Syskit::Pocolog::Streams.from_dir(dataset_dir.to_s)
        streams.each_task(app: app) do |task_streams|
            Syskit.conf.use_pocolog_task task_streams
        end
    rescue Exception
        app.cleanup
        raise
    end
    app.run
end


