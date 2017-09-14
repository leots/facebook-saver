"""
    Needs MySQL connector: https://pypi.python.org/pypi/mysql-connector-python
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

    # Connect to database
    cnx = mysql.connector.connect(user=properties["mysql_user"],
                                  password=properties["mysql_pass"],
                                  host=properties["mysql_host"],
                                  database="facebook_saver")
    cursor = cnx.cursor()

    # Add posts to the database
    for post in fb_json:
        if "message" in post:
            fb_page = "topotami"
            post_id = post["id"]
            timestamp = post["created_time"]
            message = post["message"]

            print(fb_page + " (" + timestamp + ") [" + post_id + "]: "
                  + message[0:40] + "[...]\nAdding to database...")

            # Insert post into database
            add_post = ("INSERT INTO posts "
                        "(post_id, page, message, created_time) "
                        "VALUES (%s, %s, %s, %s)")

            post_data = (post_id, fb_page, message, timestamp)

            try:
                cursor.execute(add_post, post_data)
            except mysql.connector.errors.IntegrityError:
                print("Duplicate entry! Skipping...")
        else:
            print("Story ignored...")

        print()

    # Commit changes to the database
    cnx.commit()

    # Close the cursor & database connection
    cursor.close()
    cnx.close()


if __name__ == "__main__":
    main()
