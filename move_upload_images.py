from pathlib import Path
from shutil import copy2

from common import format_pic_name

for path in Path('Resources/').rglob('*/^all/Upload/*.*'):
    new_path_str = '../../pub/media/wysiwyg/TA/descriptionImages/{}'.format(format_pic_name(path.name))
    print("Moving {} -> {}".format(str(path), new_path_str))
    copy2(str(path), new_path_str)

