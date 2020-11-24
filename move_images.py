import os
from pathlib import Path
from shutil import copy2

from common import format_pic_name

OLD_DIR = 'Resources'
NEW_DIR = 'images'

for path in Path('{}/'.format(OLD_DIR)).rglob('*/^all/*.*'):
    path_str = str(path)
    slideshow_id = path_str.split('/')[1]
    new_dir = '{}/{}'.format(NEW_DIR, slideshow_id)
    new_path_str = '{}/{}'.format(new_dir, format_pic_name(path.name))
    if not os.path.exists(new_dir):
        os.makedirs(new_dir)
    # print("Moving {} -> {}".format(path_str, new_path_str))
    copy2(path_str, new_path_str)
