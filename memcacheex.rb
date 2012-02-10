require 'rubygems'
require 'memcache'
module Debug
  DEBUG=1
  VERBOSE=2
  INFO=3
  WARNING=4
  ERROR=5
  NONE=6
  @@minl = INFO

  def dbgprint(level, msg)
    if level >= @@minl then
      case level
        when DEBUG
          print "[d] "
        when VERBOSE
          print "[v] "
        when INFO
          print "[i] "
        when WARNING
          print "[w] "
        when ERROR
          print "[E] "
      end
      puts msg
    end
  end

  def dprint(msg)
    dbgprint(Debug::DEBUG, msg)
  end

  def vprint(msg)
    dbgprint(Debug::VERBOSE, msg)
  end

  def iprint(msg)
    dbgprint(Debug::INFO, msg)
  end

  def wprint(msg)
    dbgprint(Debug::WARNING, msg)
  end

  def eprint(msg, die=false)
    dbgprint(Debug::ERROR, msg)
    exit(1) if die
  end
end
include Debug

class MemCacheEx < MemCache
  attr :version
  @caps = nil

  def slabs
    self.stats("slabs")
  end

  def settings
    self.stats("settings")
  end

  def items
    self.stats("items")
  end

  def caps
    @caps
  end



  def probe_stats_capabilities
    all_known_capabilities = [{""=>nil}, 
                              {"slabs"=>nil},
                              {"settings"=>nil},
                              {"items"=>nil},
                              {"tap"=>nil},
                              {"hash"=>nil},
                              {"vbucket"=>nil},#check is superfluous, as it requires further, unknown params
                              {"key"=>nil},#check is superfluous, as it requires further, unknown params
                              {"vkey"=>nil}]#check is superfluous, as it requires further, unknown params
    @caps = []
    all_known_capabilities.each do |cap|
      c = cap.keys[0]
      args = cap[c]
      dprint("Testing for \"#{c}\"")
      begin
        self.stats(c+(args.nil? ? "":" "+args))
        if c == "" then
          @caps << "(stat)"
        else
          @caps << c
        end
      rescue MemCache::MemCacheError
        dprint("\"#{c}\" not supported")
      end
    end
    begin
      self.set("TESTWRITEINTOTHECACHE_FROM_MEMCRASHED","yousaidyourdogdoesnotbitethatisnotmydog",0,true,123)
      @caps << "(set)"
      dprint("Successfully wrote into cache")
    rescue Exception => e
      iprint("Cannot set entries in the cache: #{e.to_s}")
    end
    begin
      flags,r = self.get("TESTWRITEINTOTHECACHE_FROM_MEMCRASHED",true)
      if r == "yousaidyourdogdoesnotbitethatisnotmydog" and flags == 123 then
        @caps << "(get)"
        dprint("Successfully pulled from the cache")
      else
        iprint("Retrieval opeation returned an unexpected value (#{r})")
      end
    rescue Exception => e
      iprint("Cannot get entries from cache: #{e.to_s}")
    end

    #@caps.each {|c| s+="#{c} "}
    vprint("Capabilities supported: #{@caps.collect{|c| "#{c} "}}")
    @caps
  end

  def debug_enable
    raise MemCacheError, "No active servers" unless active?

    complete=false
    @servers.each do |server|
      next unless server.alive?

      with_socket_management(server) do |socket|
        cmd = "stats detail on\r\n"
        socket.write cmd
        line = socket.gets
        raise_on_error_response! line
        complete=true
      end
    end

    raise MemCacheError, "No active servers" if !complete
    return true
  end

  def get_slabs_info
    #we pull two sets of slabs info from the cache, using "stats slabs" and "stats items"
    #data is merged into a single structure
    raise MemCacheError, "No active servers" unless active?
    dprint("Entered get_slabs_info")
    tmp_slabs_info = {}
    slabs=self.slabs
    slabs.keys.each do |server|
      dprint("get_slabs_info(): working on #{server}")
      slabs[server].keys.each do |i|
        dprint("get_slabs_info(): working on #{i}")
        (slabs_id,info_key) = i.split(/:/)
        info_value = slabs[server][i]
        next unless !slabs_id.nil? and !info_key.nil? and !info_value.nil?
        tmp_slabs_info[server] = {} if tmp_slabs_info[server].nil?
        tmp_slabs_info[server][slabs_id] = {} if tmp_slabs_info[server][slabs_id].nil?
        tmp_slabs_info[server][slabs_id][info_key] = info_value
      end
    end

    #now repeat, except this time pulling items stats. results format is every so slightly different
    #hence the dupped code
    slabs=self.items
    slabs.keys.each do |server|
      dprint("get_slabs_info(): working on #{server}")
      slabs[server].keys.each do |i|
        dprint("get_slabs_info(): working on #{i}")
        (not_used,slabs_id,info_key) = i.split(/:/)
        info_value = slabs[server][i]
        next unless !slabs_id.nil? and !info_key.nil? and !info_value.nil?
        tmp_slabs_info[server] = {} if tmp_slabs_info[server].nil?
        tmp_slabs_info[server][slabs_id] = {} if tmp_slabs_info[server][slabs_id].nil?
        tmp_slabs_info[server][slabs_id][info_key] = info_value
      end
    end

    tmp_slabs_info.keys.each {|s|
      tmp_slabs_info[s].keys.each {|sl|
        tmp_slabs_info[s][sl].keys.each {|info|
          dprint "#{s}:#{sl}:#{info}->#{tmp_slabs_info[s][sl][info]}"
        }
      }
    }

    tmp_slabs_info
  end


  def get_slabs_ids
    raise MemCacheError, "No active servers" unless active?
    #we deal with single servers at a time in a cache object, so we don't need to loop
    #through all possible servers (since there should be only one)
    slabs=get_slabs_info
    slabs.keys.each do |server|
      slabs[server]=slabs[server].keys
    end
    slabs
  end

  def get_keys(slabs_id, key_limit)
    raise MemCacheError, "No active servers" unless active?
    server_items = {}
    dprint("Entered get_keys(#{slabs_id},#{key_limit})")

    @servers.each do |server|
      next unless server.alive?
      dprint("Retrieving #{key_limit} keys from slabs #{slabs_id} on server #{server.host}")

      with_socket_management(server) do |socket|
        value = nil
        cmd = "stats cachedump #{slabs_id} #{key_limit}\r\n"

        socket.write cmd
        stats = {}
        while line = socket.gets do
          raise_on_error_response! line
          break if line == "END\r\n"
          if line =~ /ITEM ([\S]+) (\[.+\])/ then
            name, value = $1, $2
            stats[name] = value
          end
        end
        server_items["#{server.host}:#{server.port}"] = stats
      end
    end

    raise MemCacheError, "No active servers" if server_items.empty?
    server_items
  end

  def namespace=(namespace)
    @namespace = namespace
  end

end
