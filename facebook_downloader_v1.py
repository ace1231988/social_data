#!/usr/bin/env python
# -*- coding: utf-8 -*-

import facebook_utils_v4 as facebook_utils
from datetime import datetime
import sys


def main():
    ###Set default encoding
    reload(sys)
    sys.setdefaultencoding('utf-8')
    
    ###process starts
    print "-----------------Facebook Downloader!!! Starting at %s-----------------" %datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    ###Get database connection
    db_con = facebook_utils.get_db_connection()
    ###Get Facebook Graph API
    fb_api = facebook_utils.get_fb_api()

    ###########Update fanpage infor############
    page_id_list = facebook_utils.page_updater(db_con, fb_api)
    ############Collect fanpage posts############
    facebook_utils.new_posts_collector(page_id_list, db_con, fb_api)
    post_id_list = facebook_utils.existing_posts_updater(db_con, fb_api)
    ###########Collect fanpage comments############
    facebook_utils.new_comments_collector(post_id_list, db_con, fb_api)
    comment_id_list = facebook_utils.existing_comments_updater(db_con, fb_api)
    ###########Collect fanpage replies############
    facebook_utils.new_replies_collector(comment_id_list, db_con, fb_api)
    facebook_utils.existing_replies_updater(db_con, fb_api)
    
    db_con.close()
    
    ###process ends
    print "-----------------Facebook Downloader!!! Ended at %s-----------------" %datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
if __name__ == '__main__':
    main()

                
