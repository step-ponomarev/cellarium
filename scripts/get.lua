id = 0

request = function()
    wrk.path = "/v0/entity?q=1&id=" .. id
    wrk.method = "GET"
    id = id + 1
    return wrk.format(nil, path)
end