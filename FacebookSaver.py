"""
    Comparison of MySQL connectors for Python:
    https://stackoverflow.com/a/25724855
"""

import json

import mysql.connector


def get_local_json_contents(json_filename):
    """
    Return the contents of a (local) JSON file
    :param json_filename: the filename (as a string) of the local JSON file
    :returns: the data of the JSON file
    """

    try:
        with open(json_filename) as json_file:
            try:
                data = json.load(json_file)
            except ValueError:
                print("Contents of '" + json_filename + "' are not valid JSON")
                raise
    except IOError:
        print("An error occurred while reading the '" + json_filename + "'")
        raise

    return data


def main():
    # Read parameters from JSON properties file
    properties = get_local_json_contents("properties.json")

    # Read test JSON Facebook response
    fb_json = get_local_json_contents("test.json")
    fb_json = fb_json["data"]
    for post in fb_json:
        if "message" in post:
            fb_page = "topotami"
            timestamp = post["created_time"]
            message = post["message"]
            post_id = post["id"]

            print(fb_page + " (" + timestamp + "): " + message[0:40] + "...")

        else:
            print("Story ignored...")

    # Connect to database
    cnx = mysql.connector.connect(user=properties["mysql_user"],
                                  password=properties["mysql_pass"],
                                  host=properties["mysql_host"],
                                  database="facebook_saver")
    cnx.close()


if __name__ == "__main__":
    main()
