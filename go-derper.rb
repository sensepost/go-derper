#!/usr/bin/env ruby

require 'digest/sha1'
require "getoptlong"
#require 'rubygems'
require 'memcacheex'
require 'zlib'


#while true do
#  key=(rand*100).to_i.to_s
#  value=(rand*10000).to_i.to_s
#  CACHE.set(key,value)
#end


class Console
  def Console.clear
    $stdout.write "#{27.chr}[2J" if @@minl >= INFO
  end
end

class Leacher
  @cache=nil
  attr :server_debug_configured
   
  def set_cache(cache)
    if !cache.nil?
      throw Exception.new("Object is not a MemCache") if cache.class != MemCache and cache.class != MemCacheEx
      @cache = cache
    end
  end

 
  def Leacher(cache=nil)
    set_cache(cache)
    self.server_debug_configured = true
  end

  def open(cache=nil)
    set_cache(cache)
  end

  def canleach?
    begin
      @cache.debug_enable
      server_debug_configured = true
      return true
    rescue MemCache::MemCacheError
      return false
    end
  end

  #leach will either try to fetch a key specified with -k, or determine keys through the debug functions
  def leach(slabs_to_retrieve,key_limit,requested_key,&block)
    servers = {}

    begin
      @cache.debug_enable if !self.server_debug_configured
    rescue MemCacheError
      eprint("Could not enable debug mode on server, can't leach.")
      return false
    end

    if !requested_key.nil? then
      dprint("Only fetch a single key, \"#{requested_key}\"")
      flags,val=@cache.get(requested_key,true)
      if val.nil? then
        eprint("No entry found")
      else
        yield -1, requested_key.dup, flags, val.dup
      end
      return
    end

    if slabs_to_retrieve.nil? or slabs_to_retrieve == ""
      servers = @cache.get_slabs_ids
    else
      servers[@cache.servers[0].host+":"+@cache.servers[0].port.to_s] = [slabs_to_retrieve.to_i]
    end
    
    dprint("Starting to leach")
    servers.each do |server,slabs|
      slabs.each do |slabs_id|
        slabs_id = slabs_id.to_i if !slabs_id.is_a?(Integer)
        throw Exception.new("Slabs id (#{slabs_id.to_s}) must be an integer >= 0") if (!slabs_id.is_a?(Integer) or !(slabs_id > 0))
        dprint("Leach on slabs #{slabs_id}")
        ret=@cache.get_keys(slabs_id, key_limit)
        ret=ret[ret.keys[0]]

        @cache.namespace = nil

        ret.keys.each do |key_id|
          #if (key_id =~ /(\S+):(\S+)/) then
          #  @cache.namespace, key_id = $1, $2
          #else
          #end
          flags,val=@cache.get(key_id, true)
          dprint("#{key_id} -> #{val}")
          val = "(nil)" if val.nil?
          yield slabs_id, key_id.dup, flags, val.dup
        end
      end
    end
  end
end

class Stats
  @stats=nil
  @cache=nil
  @@header_printed=false

  def refresh_cache(submenu=nil)
    @stats = @cache.stats(submenu).shift[1] if !@cache.nil?
    throw Exception.new("stats request for #{submenu} returned nil") if @stats.nil?
    @stats
  end

  def set_cache(cache)
    if !cache.nil?
      throw Exception.new("Object (#{cache.class}) is not a MemCache") if !cache.is_a?(MemCache) or !cache.is_a?(MemCacheEx)

      throw Exception.new("Stats for #{cache.stats.length.to_i} servers found, except I'm expecting only 1") if cache.stats.length != 1
      
      @cache = cache
      
      refresh_cache
    end
  end

  def Stats(cache=nil)
    set_cache(cache)
  end

  def Stats(server, port, namespace)
    set_cache(MemCache.new(server+":"+port,:namespace => namespace))
  end

  def open(cache=nil)
    if @stats.nil?
      if cache.nil?
        Exception.new("No stored or supplied cache object")
      else
        set_cache(cache)
      end
    end
  end

  def method_missing(meth)
    throw Exception.new("No cache object") if @stats.nil?
    refresh_cache
    
    throw NoMethodError.new("undefined method `#{meth.id2name}'") if @stats[meth.id2name].nil?
    @stats[meth.id2name]
  end

  def get_field(field)
    throw Exception.new("No cache object") if @stats.nil?

    return all_fields if field == "all"

    throw Exception.new("No such field #{field}") if @stats[field].nil?
    @stats[field]
  end

  def all_fields
    throw Exception.new("No cache object") if @stats.nil?
    str=""
    @stats.keys.sort.each {|k| str+= "#{k} -> #{@stats[k]}\n"}
    str
  end

  def print_all_fields
    throw Exception.new("No cache object") if @stats.nil?
    @stats.keys.sort.each do |k|
      dbgprint INFO, "#{k} -> #{@stats[k]}"
    end
  end

  @@lengths={:k=>0,:c=>0,:p=>0,:r=>0,:t=>0,:m=>0}
  def print_monitored_data data
    keys = data[:base].keys.sort

    t_time = data[:total_elapsed_time]
    p_time = data[:poll_elapsed_time]

    l=@@lengths

    Console.clear

    puts "Monitoring memcached on #{@cache.servers[0].host}:#{@cache.servers[0].port}, monitor uptime is #{"%.2fs" %data[:total_elapsed_time]}\n\n"
    keys.each do |k|
      #puts "#{k}: prev change #{data[:changed][k]} in #{"%.2f" % t} s, running totals #{data[:running][k]}, total movement #{data[:base][k]} ("+ ("%.2f" % (data[:base][k].to_f/t)) +"/s)"
      l[:k] = k.length+1 if k.length>l[:k]
      c="current #{data[:current][k]}"
      l[:c] = c.length+1 if c.length>l[:c]
      p="prev change #{data[:changed][k]} in #{"%.2f" % p_time}s"
      l[:p] = p.length+1 if p.length>l[:p]
      r="running totals #{data[:running][k]}"# (avg #{"%.2f" % (data[:running][k].to_f/p_time)} chgs/s)"
      l[:r] = r.length+1 if r.length>l[:r]
      t="total movement #{data[:base][k]} ("+ ("%.2f" % (data[:base][k].to_f/t_time)) +"/s)"
      l[:t] = t.length+1 if t.length>l[:t]
      m="biggest movement #{data[:movers][k][:move]} ("+ ("%.2f" % (Time.now-data[:movers][k][:time_distance]).to_f) +"s ago)"
      l[:m] = m.length+1 if m.length>l[:m]

      printf "%-#{l[:k]}s: %-#{l[:c]}s %-#{l[:p]}s %-#{l[:r]}s %-#{l[:t]}s %-#{l[:m]}s\n", k, c, p, r, t, m
    end
    
  end

  def monitor(limit,submenu,gap)
    dprint "Entered #{caller(0)[0]}"
    #we'll track every key that changed in the course of monitoring, not just singe the last poll
    changed_keys=[]
    base_measure=refresh_cache(submenu).dup
    running_totals={}
    changed_totals=nil
    dprint "Base measure obtained"
    base_time=Time.now
    first_time=nil
    poll_time=nil
    biggest_movers={}

    first_measure=refresh_cache(submenu).dup
    while limit >= 0
      first_time = Time.now
      sleep(gap)
      poll_measure=refresh_cache(submenu).dup
      poll_time=Time.now
      total_elapsed_time = poll_time - base_time
      poll_elapsed_time = poll_time - first_time

      dprint "Poll measure obtained"

      changed_totals={}
      base_measure.keys.sort.each do |k|
        begin
          Integer(base_measure[k])
          Integer(first_measure[k])
          Integer(poll_measure[k])
          diff=poll_measure[k].to_i-first_measure[k].to_i
          next if diff == 0
          changed_keys<<k if !changed_keys.member? k
          running_totals[k] = 0 if running_totals[k].nil?
          running_totals[k] += (diff.abs)
          changed_totals[k] = diff

          biggest_movers[k] = {:time_distance=>0, :move=>0} if biggest_movers[k].nil?
          if diff.abs > biggest_movers[k][:move].abs then
            biggest_movers[k][:time_distance]=poll_time
            biggest_movers[k][:move]=diff
          end

          
          dprint "#{k}: #{diff} (#{diff.to_f/poll_elapsed_time} changes/sec)"
        rescue ArgumentError
        end
      end

      monitored_data={:total_elapsed_time => total_elapsed_time, :poll_elapsed_time => poll_elapsed_time,:current=>{},:base =>{},:running=>{},:changed=>{},:movers=>{}}
      changed_keys.each do |k|
        throw Exception.new("Unexpected absent key #{k}") if base_measure[k].nil? or poll_measure[k].nil?
        monitored_data[:current][k] = poll_measure[k].to_i
        monitored_data[:base][k] = (poll_measure[k].to_i-base_measure[k].to_i).to_f
        monitored_data[:running][k] = running_totals[k].to_f
        monitored_data[:changed][k] = changed_totals[k]
        monitored_data[:movers][k] = biggest_movers[k]

      end

      print_monitored_data monitored_data

      if limit == 1
        limit -= 2
      elsif limit > 0
        limit -= 1
      end

      first_measure=poll_measure
    end
    dprint "Left #{caller(0)[0]}"
  end

  def pretty_time(t)
    return if t.nil?
    #arb constant to separate sec-since-epoch from plain seconds counts
    if t > 1050000000 then
      return Time.at(t).to_s
    end
  
    s=""
    sec=0
    min=0
    hour=0
    day=0
    t=t.to_f

    if t / (24 * 3600) >= 1 then
      day=(t / (24 * 3600)).floor
      t-=day*(24*3600)
    end
    if t / (3600) >= 1 then
      hour=(t / (3600)).floor
      t-=hour*(3600)
    end
    if t / (60) >= 1 then
      min=(t / (60)).floor
      t-=min*(60)
    end
    sec=t

    sprintf("%i:%02i:%02i:%02i",day,hour,min,sec)
  end

  def pretty_num(m)
    return if m.nil?
    if m > 10**12
      return sprintf("%.2ft",m.to_f/10**12)
    elsif m > 10**9
      return sprintf("%.2fb",m.to_f/10**9)
    elsif m > 10**6
      return sprintf("%.2fm",m.to_f/10**6)
    elsif m > 10**3
      return sprintf("%.2fk",m.to_f/10**3)
    else
      return sprintf("%i",m)
    end
  end

  def pretty_mem(m)
    return if m.nil?
    if m > 2**40
      return sprintf("%.2f TB",m.to_f/2**40)
    elsif m > 2**30
      return sprintf("%.2f GB",m.to_f/2**30)
    elsif m > 2**20
      return sprintf("%.2f MB",m.to_f/2**20)
    elsif m > 2**10
      return sprintf("%.2f KB",m.to_f/2**10)
    else
      return sprintf("%i B",m)
    end
  end

  def print_fingerprint(fp,output_format)
    #puts "\n\n#{@cache.servers[0].host}:#{@cache.servers[0].port}\n=============================="
    if @@header_printed == true then
      dprint("@@header_printed = true")
    elsif @@header_printed == false then
      dprint("@@header_printed = false")
    else
      dprint("@@header_printed = "+@@header_printed) 
    end

    if output_format == "csv" then
      if @@header_printed == false then
        puts "Host,Version,PID,Uptime,Systime,Utime,Stime,Max Bytes,Max Item Size,Current Connections,Net Bytes Read,Net Bytes Written,Get Count,Set Count,Bytes Stored,Item Count,Total Items,Total Slabs,Stats Capabilities"
        @@header_printed = true
      end
      puts "#{@cache.servers[0].host}:#{@cache.servers[0].port},#{fp[:runenv][:version]},#{fp[:runenv][:pid]},"+pretty_time(fp[:runenv][:uptime])+","+pretty_time(fp[:runenv][:time])+",#{"%.2f" % fp[:runenv][:rusage_user]},#{"%.2f" % fp[:runenv][:rusage_system]},#{fp[:memory][:maxbytes]},#{fp[:memory][:item_size_max]},#{fp[:runenv][:curr_connections]},#{fp[:network][:bytes_read]},#{fp[:network][:bytes_written]},#{fp[:cache][:cmd_get]},#{fp[:cache][:cmd_set]},#{fp[:cache][:bytes]},#{fp[:cache][:curr_items]},#{fp[:cache][:total_items]},#{fp[:cache][:total_slabs]},#{fp[:runenv][:capabilities].join(" ")}"
    elsif output_format == "multiline" then
      puts "#{@cache.servers[0].host}:#{@cache.servers[0].port}\n=============================="

      puts "memcached #{fp[:runenv][:version]} (#{fp[:runenv][:pid]}) up "+pretty_time(fp[:runenv][:uptime])+", sys time "+pretty_time(fp[:runenv][:time])+", utime=#{"%.2f" % fp[:runenv][:rusage_user]}, stime=#{"%.2f" % fp[:runenv][:rusage_system]}"
      puts "Mem: Max #{pretty_mem(fp[:memory][:maxbytes])}, max item size = #{pretty_mem(fp[:memory][:item_size_max])}"
      puts "Network: curr conn #{fp[:runenv][:curr_connections]}, bytes read #{pretty_mem(fp[:network][:bytes_read])}, bytes written #{pretty_mem(fp[:network][:bytes_written])}"
      puts "Cache: get #{pretty_num(fp[:cache][:cmd_get])}, set #{pretty_num(fp[:cache][:cmd_set])}, bytes stored #{pretty_mem(fp[:cache][:bytes])}, curr item count #{pretty_num(fp[:cache][:curr_items])}, total items #{pretty_num(fp[:cache][:total_items])}, total slabs #{pretty_num(fp[:cache][:total_slabs])}"
      puts "Stats capabilities: #{fp[:runenv][:capabilities].join(" ")}"
      puts ""
    end
    
  end

  def process_items(items_in)
    items_out = {}
    total_items=0
    items_in.keys.each do |k|
      (prefix,s_num,label) = k.split(/:/)
      throw Exception.new("stats data malformed, can't split(/,/) \"#{k}\" into three") if prefix.nil? or s_num.nil? or label.nil?
      s_num = s_num.to_i
      items_out[s_num] = {} if items_out[s_num].nil?

      items_out[s_num][label.to_sym] = items_in[k]
  
      total_items+= items_in[k].to_i if label.to_sym == :number
    end
    {:items=>items_out,:total_items=>total_items,:total_slabs=>items_out.length}
  end

  def fingerprint
    fp = {:runenv=>{},:memory=>{},:cache=>{},:network=>{}}
    
    fp[:runenv][:capabilities]=@cache.probe_stats_capabilities
    data=refresh_cache("").dup
    fp[:runenv][:pid]=data["pid"]
    fp[:runenv][:uptime]=data["uptime"]
    fp[:runenv][:time]=data["time"]
    fp[:runenv][:version]=data["version"]
    fp[:runenv][:rusage_user]=data["rusage_user"]
    fp[:runenv][:rusage_system]=data["rusage_system"]
    fp[:runenv][:curr_connections]=data["curr_connections"]
    fp[:network][:bytes_read]=data["bytes_read"]
    fp[:network][:bytes_written]=data["bytes_written"]
    fp[:cache][:cmd_get]=data["cmd_get"]
    fp[:cache][:cmd_set]=data["cmd_set"]
    fp[:cache][:bytes]=data["bytes"]
    fp[:cache][:curr_items]=data["curr_items"]


    begin
      data=refresh_cache("settings").dup
      fp[:memory][:maxbytes]=data["maxbytes"]
      fp[:memory][:item_size_max]=data["item_size_max"]
    rescue MemCache::MemCacheError
      fp[:memory][:maxbytes]=-1
      fp[:memory][:item_size_max]=-1
    end

    begin
      data=refresh_cache("items").dup
      items=process_items(data)
      fp[:cache][:total_items]=items[:total_items]
      fp[:cache][:total_slabs]=items[:total_slabs]
    rescue MemCache::MemCacheError
      fp[:cache][:total_items]=-1
      fp[:cache][:total_slabs]=-1
    end
    fp
  end

  def get_version
    self.get_field("version")
  end
end

def usage
  puts "
go-derper.rb v0.11 (c) marco@sensepost.com

\t-h\thelp
\t-s\t<server address>
\t-p\t<port>
\t-n\t<namespace>
\t-S\tstats summary mode
\t-t\t<comms timeout>
\t-L\tlist all slabs
\t-l\tleach mode <slabs_id>
\t-k\t<key-to-retrieve>
\t-d\t<cache_content_file_to_delete>
\t-K\t<number of keys to pull per slabs_id> (leach mode)
\t-R\t<file-containing-regexes>
\t-m\t<stats menu to monitor> monitor mode
\t-M\t<monitor gap timing> (monitor mode)
\t-f\t<comma separated list of servers for fingerprinting>
\t-F\t<file containing servers, one IP per line, for fingerprinting>
\t-c\t[ csv | multiline ] fingerprint output format (multiline is default)
\t-o\t<output_directory> (must exist)
\t-i\tinclude the slabs_id in output filename
\t-r\t<output_file_prefix>
\t-v\tverbose (multiple for more)
\t-w\t<cache_content_file_to_write>
\t-z\tdetect and expand zlib streams
  "
end

opt = GetoptLong.new(
  ["--help", "-h", GetoptLong::NO_ARGUMENT],
  ["--server", "-s", GetoptLong::REQUIRED_ARGUMENT],
  ["--port", "-p", GetoptLong::OPTIONAL_ARGUMENT],
  ["--namespace", "-n", GetoptLong::REQUIRED_ARGUMENT],
  ["--stats", "-S", GetoptLong::OPTIONAL_ARGUMENT],
  ["--timeout", "-t", GetoptLong::REQUIRED_ARGUMENT],
  ["--leach", "-l", GetoptLong::OPTIONAL_ARGUMENT],
  ["--keylimit", "-K", GetoptLong::REQUIRED_ARGUMENT],
  ["--key", "-k", GetoptLong::REQUIRED_ARGUMENT],
  ["--delete-key", "-d", GetoptLong::REQUIRED_ARGUMENT],
  ["--regexes", "-R", GetoptLong::REQUIRED_ARGUMENT],
  ["--monitor", "-m", GetoptLong::OPTIONAL_ARGUMENT],
  ["--monitor-gap", "-M", GetoptLong::REQUIRED_ARGUMENT],
  ["--fingerprint", "-f", GetoptLong::REQUIRED_ARGUMENT],
  ["--fingerprint-file", "-F", GetoptLong::REQUIRED_ARGUMENT],
  ["--fingerprint-output","-c", GetoptLong::REQUIRED_ARGUMENT],
  ["--output-directory", "-o", GetoptLong::REQUIRED_ARGUMENT],
  ["--output-prefix", "-P", GetoptLong::REQUIRED_ARGUMENT],
  ["--quiet", "-q", GetoptLong::NO_ARGUMENT],
  ["--list-slabs", "-L", GetoptLong::NO_ARGUMENT],
  ["--include-slabs-id-in-filename", "-i", GetoptLong::NO_ARGUMENT],
  ["--zlib-expand", "-z", GetoptLong::NO_ARGUMENT],
  ["--verbose", "-v", GetoptLong::NO_ARGUMENT],
  ["--write", "-w", GetoptLong::REQUIRED_ARGUMENT]
)

server = nil
port = 11211
namespace = nil
mode = nil
mode_inputs = nil
timeout = 5
monitor_gap = 10
key_limit = 10
output_directory = ""
output_prefix = ""
include_slabs_id_in_filename = false
zlib_expand = false
fingerprint_output="multiline"
requested_key=nil
delete_key=nil
regexes=[]


opt.each do |opt, arg|
  dprint "opt=#{opt},arg=#{arg}"
  case opt
    when "--help"
      usage
      exit(1)
    when "--server"
      server = arg
    when "--port"
      port = arg
    when "--namespace"
      namespace = arg
    when "--stats"
      eprint "Only one mode allowed, trying to overwrite #{mode.id2name} mode with #{opt}", true if !mode.nil?
      mode = :stats
      if arg.nil? or arg.length == 0
        dprint "No args, setting to \"all\""
        mode_inputs = "all"
      else
        dprint "Arg found, setting to \"#{arg}\""
        mode_inputs = arg 
      end
    when "--monitor"
      eprint "Only one mode allowed, trying to overwrite #{mode.id2name} mode with #{opt}", true if !mode.nil?
      mode = :monitor
      mode_inputs = "" #default
      mode_inputs = arg if !arg.nil?
    when "--monitor-gap"
      monitor_gap = arg.to_i
    when "--timeout"
      timeout = arg.to_i
    when "--verbose"
      @@minl -= 1 if @@minl > 1
    when "--fingerprint"
      eprint "Only one mode allowed, trying to overwrite #{mode.id2name} mode with #{opt}", true if !mode.nil?
      mode = :fingerprint
      mode_inputs = arg
    when "--fingerprint-file"
      eprint "Only one mode allowed, trying to overwrite #{mode.id2name} mode with #{opt}", true if !mode.nil?
      mode = :fingerprintfile
      mode_inputs = arg
      begin
        f=File.open(mode_inputs)
        f.close
      rescue Exception => e
        eprint("Could not open file for -F: #{e.to_s}")
        exit(1)
      end
    when "--fingerprint-output"
      if arg != "multiline" and arg != "csv" then
        eprint("--fingerprint-output can be either \"multiline\" or \"csv\", but not \"#{arg}\"")
        exit(1)
      end
      fingerprint_output = arg
      @@minl = WARNING if arg == "csv" #raise level to warning or above when writing .csv
    when "--leach"
      eprint "Only one mode allowed, trying to overwrite #{mode.id2name} mode with #{opt}", true if !mode.nil?
      mode = :leach
      mode_inputs = 0 #default
      mode_inputs = arg if !arg.nil?
    when "--list-slabs"
      eprint "Only one mode allowed, trying to overwrite #{mode.id2name} mode with #{opt}", true if !mode.nil?
      mode = :list_slabs
      mode_inputs = 0 #default
    when "--delete-key"
      eprint "Only one mode allowed, trying to overwrite #{mode.id2name} mode with #{opt}", true if !mode.nil?
      mode_inputs = arg
      mode = :delete_key
    when "--output-directory"
      output_directory = arg
    when "--output-prefix"
      output_prefix = arg
    when "--include-slabs-id-in-filename"
      include_slabs_id_in_filename = true
    when "--key"
      requested_key = arg
    when "--keylimit"
      key_limit = arg.to_i
      dprint "Keylimit set to #{key_limit}"
    when "--regexes"
      begin
        f=File.open(arg)
        f.readlines.join.split(/\n/).each do |regex|
          next if regex == "" or regex =~ /^#/
          regexes << Regexp.new(regex, Regexp::IGNORECASE | Regexp::MULTILINE)
        end
        f.close
        dprint("Loaded #{regexes.length} regular expressions")
      rescue Exception => e
        eprint("Could not open file for -r: #{e.to_s}")
        exit(1)
      end
    when "--quiet"
      @@minl = NONE
    when "--write"
      eprint "Only one mode allowed, trying to overwrite #{mode.id2name} mode with #{opt}", true if !mode.nil?
      mode = :writeentry
      mode_inputs = arg
      begin
        f=File.open(mode_inputs)
        f.close
      rescue Exception => e
        eprint("Could not open file for -w: #{e.to_s}")
        exit(1)
      end
      begin
        index_filename=File.dirname(mode_inputs)+"/"+File.basename(mode_inputs).split(/-/)[0]+"-index"
        f=File.open(index_filename)
      rescue Errno::NOENT
        eprint("Could not determine index file (#{index_filename}) for #{mode_inputs}")
        exit(1)
      end
    when "--zlib-expand"
      zlib_expand = true
  end
end

#default mode
if mode.nil? then
  mode = :stats 
  mode_inputs = "all"
end

if (mode == :monitor or mode == :stats) and (server.nil? or port.nil? or namespace.nil?) then
  eprint "I need a server, port and namespace. Until such time, i refuse to run."
  usage
  exit(1)
end

dprint "Running in mode #{mode.id2name} with input #{mode_inputs}"


case mode
  when :stats
    CACHE = MemCacheEx.new "#{server}:#{port}", :namespace => namespace, :timeout => timeout
    s=Stats.new
    s.open(CACHE)
    dbgprint INFO, s.get_field(mode_inputs)
  when :monitor
    CACHE = MemCacheEx.new "#{server}:#{port}", :namespace => namespace, :timeout => timeout
    s=Stats.new
    s.open(CACHE)
    s.monitor(0, mode_inputs,monitor_gap)
  when :fingerprint
    mode_inputs.split(/,/).each do |server|
      begin
        iprint "Scanning #{server}"
        s=Stats.new
        s.open(MemCacheEx.new("#{server}", :namespace => namespace, :timeout => timeout))
        s.print_fingerprint(s.fingerprint,fingerprint_output)
      rescue Exception => e
        puts e.to_s
      end
    end
  when :fingerprintfile
    File.open(mode_inputs).readlines.join.split(/\n/).each do |server|
      begin
        iprint "Scanning #{server}"
        s=Stats.new
        s.open(MemCacheEx.new("#{server}", :namespace => namespace, :timeout => timeout))
        s.print_fingerprint(s.fingerprint,fingerprint_output)
      rescue Exception => e
        puts e.to_s
      end
    end
  when :leach
    #let's make sure we can write data first
    if output_directory == ""
      wprint("No output directory specified, defaulting to ./output")
      output_directory = "output"
    end
    
    begin
      Dir.new(output_directory)
    rescue Errno::ENOENT
      eprint("Directory #{output_directory} does not exist, please create it and run me again.")
      exit(1)
    end

    if output_prefix == ""
      i=0
      begin
        i+=1
        output_prefix = "run"+i.to_s
      end while File::exists?(output_directory+"/"+output_prefix+"-index")
      wprint("No prefix supplied, using \"#{output_prefix}\"")
    end

    #now conncet and pull from the cache
    if server.nil? or port.nil?
      eprint("-l requires a server specified with -s")
      exit(1)
    end

#initialise the cache object
    CACHE = MemCacheEx.new "#{server}:#{port}", :namespace => namespace, :timeout => timeout
    l=Leacher.new
    l.open(CACHE)
    dprint "Leaching slabs_id #{mode_inputs} for #{key_limit} keys"
    if l.canleach? then
#start leaching. heavy-lifting of retrieving keys and values falls to class Leach. here, we just supply
#a handler for outputting the key,val pair to disk. we also handle zlib streams, if we find them
      l.leach(mode_inputs,key_limit,requested_key) do |slabs_id, key, flags, value|
        if !value.nil? and value.length >= 2 && zlib_expand && value[0] == 0x78 and value[1] == 0x9c then
          value = Zlib::Inflate.inflate(value)
          dprint("Inflated value is: #{value}")
          slabs_id = "z_"+slabs_id.to_s
        end
        key.gsub!(/,/,"IWASACOMMA")
#each entry is also recorded in the index file, which saves the orginating server and key
        filename = output_prefix+"-"+Digest::SHA1.hexdigest(key)
        index_entry = "#{server}:#{port},#{slabs_id.to_s},#{key},#{flags},#{value.length},#{output_directory}/#{filename}\n"
        begin
          f_i=File.open("#{output_directory}/#{output_prefix}-index","a")
          f_i.write(index_entry)
          f_i.close

          f_k=File.new("#{output_directory}/#{filename}","w")
          f_k.write(value)
          f_k.close
          
#if there are any regex matches, print to screen now
          regexes.each do |regex|
            begin
              m = regex.match(value)
              skipped=0
              1.upto(m.length-1) do |i|
                dprint("Matched token (i="+i.to_s+"): "+m[i])
                next if i!=1 and m[1].include?(m[i])
                iprint("Found in #{output_directory}/#{filename} at #{m.begin(i)+skipped} -> #{m[i]}") 
              end if m
              if m
                skipped+=m.end(0)
                value = value[m.end(0)..-1]
              end
            end while m
          end
        rescue Errno::ENOENT
          eprint("Failed to create the output file. Does the directory exist? Exiting.")
          exit(1)
        end
        vprint("leached from slab #{slabs_id.to_s} #{filename}  -> #{key} (#{value.length.to_s} bytes)")
      end
    else
      eprint("Can't enable debug mode on server, leaching only through brute-force is currently unsupported")
    end
  when :list_slabs
    cache = MemCacheEx.new "#{server}:#{port}", :namespace => namespace, :timeout => timeout
    cache.get_slabs_info.each do |server,slabs|
      slabs.keys.each do |slabs_id|
        s = ""
        s += "\t#{slabs[slabs_id]["number"]} (Cache entries)" if !slabs[slabs_id]["number"].nil?
        s += "\t#{slabs[slabs_id]["chunk_size"]} (Chunk size)" if !slabs[slabs_id]["chunk_size"].nil?
        s += "\t#{slabs[slabs_id]["mem_requested"]} (Mem requested)" if !slabs[slabs_id]["mem_requested"].nil?
        iprint("\t#{server}\t#{slabs_id}#{s}")
      end
    end
  when :writeentry
    #First, determine which server and key we're going to write to. This is stored in the index file
    entry_filename = mode_inputs
    index_filename=File.dirname(entry_filename)+"/"+File.basename(entry_filename).split(/-/)[0]+"-index"
    dprint("Checking in #{index_filename}")
    server_and_port,slabs_id,key,flags,entry_size,filename = "","","","","",""
    begin
      f_i=File.open(index_filename)
      indexes=f_i.readlines.join.split("\n")
      found=false
      indexes.each do |index_line|
        (server_and_port,slabs_id,key,flags,entry_size,filename) = index_line.split(/,/)
        key.gsub!(/IWASACOMMA/,",")
        if entry_filename == filename then
          found=true
          break 
        end
      end
      f_i.close
      if found == false
        eprint("Could not find an entry for #{entry_filename} in index file #{index_filename}")
        exit(1)
      end
      server,port = server_and_port.split(/:/)
      port = port.to_i
      flags=flags.to_i
      if server == "" or !(port>0) or slabs_id == "" or key == "" then
        eprint("Unknown error led to server,slabs_id or key being empty, or the port number was incorrect")
        exit(1)
      end
      f_e = File.open(entry_filename)
      entry = f_e.read(File.stat(entry_filename).size)
      dprint("entry is #{File.stat(entry_filename).size} bytes")
      f_e.close
      vprint("Setting entry on server #{server}:#{port.to_s} with key \"#{key}\" and flags \"#{flags}\"")
      dprint("Entry value is \"#{entry}\"")
      cache = MemCacheEx.new "#{server}:#{port}", :namespace => (namespace==""?nil:namespace), :timeout => timeout
      cache.set(key,entry,0,true,flags)
    rescue Errno::ENOENT => e
      eprint("An error occurred: #{e.to_s}")
    end
  when :delete_key
    entry_filename = mode_inputs
    index_filename=File.dirname(entry_filename)+"/"+File.basename(entry_filename).split(/-/)[0]+"-index"
    dprint("Checking in #{index_filename}")
    server_and_port,slabs_id,key,flags,entry_size,filename = "","","","","",""
    begin
      f_i=File.open(index_filename)
      indexes=f_i.readlines.join.split("\n")
      found=false
      indexes.each do |index_line|
        (server_and_port,slabs_id,key,flags,entry_size,filename) = index_line.split(/,/)
        key.gsub!(/IWASACOMMA/,",")
        if entry_filename == filename then
          found=true
          break 
        end
      end
      f_i.close
      if found == false
        eprint("Could not find an entry for #{entry_filename} in index file #{index_filename}")
        exit(1)
      end
      server,port = server_and_port.split(/:/)
      port = port.to_i
      flags=flags.to_i
      if server == "" or !(port>0) or slabs_id == "" or key == "" then
        eprint("Unknown error led to server,slabs_id or key being empty, or the port number was incorrect")
        exit(1)
      end

      cache = MemCacheEx.new "#{server}:#{port}", :namespace => (namespace==""?nil:namespace), :timeout => timeout
      cache.delete(key)
    rescue Errno::ENOENT => e
      eprint("An error occurred: #{e.to_s}")
    end
  else
    eprint "Unknown mode"
end
