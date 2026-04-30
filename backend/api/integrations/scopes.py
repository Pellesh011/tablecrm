import json


async def parse_openapi_json():
    data = []
    with open("/backend/openapi.json", "r") as f:
        openapi = dict(json.load(f))
        for path, val in openapi["paths"].items():
            for k, v in val.items():
                full_path = ""
                descr = ""
                data_1 = {}
                if k in ("get", "post", "put", "patch", "delete"):
                    full_path = path + "." + k
                    data_1["scope"] = full_path
                    descr = openapi["paths"][path][k]["summary"]
                    data_1["interaction"] = descr
            data.append(data_1)
    return data
