import requests
from flask import Flask
from flask import jsonify


instance_metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/"
metadata_entries_to_grab = {"id": "instance-id", "name": "instance-name"}
metadata_flavor_header = {"Metadata-Flavor": "Google"}

app = Flask(__name__)


@app.route('/')
def main():
    return_resp = {"message": "hello world"}
    return jsonify(return_resp)


# @app.route('/')
# def main():
#     return_resp = {}
#
#     for metadata_entry in metadata_entries_to_grab:
#         req = "{}{}".format(instance_metadata_url, metadata_entry)
#         resp = requests.get(req, headers=metadata_flavor_header)
#         return_resp[metadata_entries_to_grab[metadata_entry]] = resp.text
#
#     print(return_resp)
#     return jsonify(return_resp)
#

# if __name__ == '__main__':
#     app.run(host="0.0.0.0", port=80)
