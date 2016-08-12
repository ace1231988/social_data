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
    print "-----------------Fanpage Init!!! Starting at %s-----------------" %datetime.now().strftime('%Y-%m-%d %H:%M:%S')


    ###Specify keywords interested
    fanpage_list = ['viewqwest']
    # fanpage_list = ['myrepublicsg']    
    # fanpage_list = ['singtel', 'starhub', 'mobile1']    
    ###Get database connection
    db_con = facebook_utils.get_db_connection()
    ###Get Facebook Graph API
    fb_api =  facebook_utils.get_fb_api()
    
    ###get fangapge infor
    fanpage_infor = []
    for fanpage in fanpage_list:
        print 'fanpage name: %s' %fanpage
        fanpage_infor.append(facebook_utils.get_page_infor(fanpage, fb_api))
    ###write fanpage infor to database
    facebook_utils.write2db_page_infor(fanpage_infor, db_con)
        
    db_con.close()
    
    ###process ends
    print "-----------------Fanpage Init!!! Ended at %s-----------------" %datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
if __name__ == '__main__':
    main()

                
