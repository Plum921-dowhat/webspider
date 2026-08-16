local total = 0
local json_fmt = 0
local plain_fmt = 0
local escaped_fmt = 0
local no_url = 0
local seen = {}
local samples = {}
local start_id = '-'
while true do
  local items = redis.call('XRANGE', KEYS[1], start_id, '+', 'COUNT', 5000)
  if #items == 0 then break end
  for i, v in ipairs(items) do
    local msg = v[2][2]
    local id = v[1]
    start_id = '(' .. id
    total = total + 1
    local url = nil
    -- 标准 JSON: "url":"..." 或 "url": "..."
    url = string.match(msg, '"url"%s*:%s*"([^"]+)"')
    if url then
      json_fmt = json_fmt + 1
    else
      -- 无引号: {url:https://...
      url = string.match(msg, 'url[:：]https?://[^,%s}]+')
      if url then
        plain_fmt = plain_fmt + 1
      else
        -- 转义格式: url\\:https 或 url\\":...
        url = string.match(msg, 'url\\+["]?[:：]["]?\\*(https?://[^\\,%s}]+)')
        if url then
          escaped_fmt = escaped_fmt + 1
        else
          no_url = no_url + 1
          if #samples < 4 then samples[#samples+1] = string.sub(msg, 1, 110) end
        end
      end
    end
    if url then seen[url] = (seen[url] or 0) + 1 end
  end
end
local uniq = 0
local dup_urls = {}
for u, c in pairs(seen) do
  uniq = uniq + 1
  if c > 1 then table.insert(dup_urls, c .. 'x ' .. string.sub(u, 1, 90)) end
end
table.sort(dup_urls, function(a, b)
  local ca = tonumber(string.match(a, '^(%d+)')) or 0
  local cb = tonumber(string.match(b, '^(%d+)')) or 0
  return ca > cb
end)
local out = {}
for i = 1, math.min(20, #dup_urls) do out[#out+1] = dup_urls[i] end
return {'total='..total, 'json='..json_fmt, 'plain='..plain_fmt, 'escaped='..escaped_fmt,
        'no_url='..no_url, 'uniq_url='..uniq,
        'top_dup='..table.concat(out, ' || '),
        'no_url_samples='..table.concat(samples, ' || ')}
