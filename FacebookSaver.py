"""
    Needs MySQL connector: https://pypi.python.org/pypi/mysql-connector-python
"""

import json

import mysql.connector
import requests


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


def save_posts_to_db(properties, page, posts):
    """
    Save the given posts into a MySQL database

    :param page: ID of the FB page that the posts were retrieved from
    :param properties: Database host, username and password
    :param posts: The posts, as retrieved from the Facebook API
    :return: -
    """

    no_more_pages = False

    # Connect to database
    cnx = mysql.connector.connect(user=properties["mysql_user"],
                                  password=properties["mysql_pass"],
                                  host=properties["mysql_host"],
                                  database=properties["mysql_db"])
    cursor = cnx.cursor()

    # Add posts to the database
    for post in posts:
        fb_page = page
        post_id = post["id"]
        message = post.get("message", None)
        story = post.get("story", None)
        timestamp = post["created_time"]

        # Insert post into database
        add_post = ("INSERT INTO posts "
                    "(post_id, page, message, story, created_time) "
                    "VALUES (%s, %s, %s, %s, %s)")

        post_data = (post_id, fb_page, message, story, timestamp)

        try:
            cursor.execute(add_post, post_data)
        except mysql.connector.errors.IntegrityError as err:
            print("Duplicate entry! Stopping for page. (" + err.msg + ")")

            no_more_pages = True
            break

    # Commit changes to the database
    cnx.commit()

    # Close the cursor & database connection
    cursor.close()
    cnx.close()

    # Return whether there are more posts or not
    return no_more_pages


def get_facebook_posts(token, page_id, offset, limit):
    """
    Get a number of Facebook posts of a page using the Facebook Graph API,
    with the given offset.

    :param token: Facebook API token
    :param page_id: The ID of the page to get posts of
    :param offset: Where to start getting posts from
    :param limit: How many posts to fetch. Upper limit from Facebook is 100.
    :return: The retrieved posts, in JSON format
    """

    url = "https://graph.facebook.com/v2.10/" + page_id + "/feed"

    request_params = {
        "access_token": token,
        "offset": offset,
        "limit": limit
    }

    r = requests.get(url, params=request_params)

    # Return the response, parsed as JSON
    return json.loads(r.text)


def main():
    # Read parameters from JSON properties file
    properties = get_local_json_contents("properties.json")
    fb_api_token = properties["facebook_token"]
    pages_to_watch = properties["facebook_pages"]

    page_size = 100

    # Get Facebook posts for each of the pages
    for page in pages_to_watch:
        print("Getting posts for page: " + page)

        offset = 0
        while True:
            # Get Facebook posts
            print("Requesting posts from " + str(offset) + " to "
                  + str(offset + page_size) + "...")

            fb_json = get_facebook_posts(fb_api_token, page, offset, page_size)
            fb_json = fb_json["data"]

            print("\t" + str(len(fb_json)) + " posts on this page")

            # Save these posts to the database
            done = save_posts_to_db(properties, page, fb_json)

            # If a duplicate post was found or there are no more posts, move on
            if done or len(fb_json) == 0:
                print("Done with " + page + ", moving to next site\n\n")
                break
            else:
                offset += page_size


if __name__ == "__main__":
    main()
