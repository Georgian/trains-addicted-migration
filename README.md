Inspired by: https://bitbucket.org/mariusmagureanu/vescu.git 

### export.py

Crème de la crème.
Deserializes, rearanges and sanitizes data from a Joomla db.sqlite database. Output is a Magento 2.x readable CSV file.
Place db.sqlite next to script and run it. 

Features include:
- Sanitizes image URLs (see the other scripts)
- Builds unique url_key(s)
- Maps categories to Magento-compliant ones (see category_mappings.csv)
- Replaces name with other info in case it's missing
- File batching for a more granular Magento import

### move_images.py

Copies images from Resources folder into 'images' folder in order to be read by Magento when importing products.
Image names are sanitized, because Magento does not like spaces and other characters in paths. That's the reason
why this script was written in the first place, because of the caret character in the original path of each image.

Example:
- old path: Reources/^all/17493698/macara-edk-750-OBB-Roco-73036-.jpg (notice the caret...)
- new path: var/import/images/17493698/macara-edk-750-OBB-Roco-73036-.jpg

### move_upload_images.py

Some product descriptions contain images. Their original path was in the Resources/{id}/^all/Upload folder.
This script replaces that path with a new one, where Magento stores manually uploaded images.

Example:
- old: Resources/17493698/^all/Upload/macara-edk-750-db-Roco-73035-a-.jpg
- new: pub/media/wysiwyg/TA/descriptionImages/macara-edk-750-db-Roco-73035-a-.jpg

### category_mappings.csv

Maps old categories to Magento-compliant categories and subcategories.

### common.py and serialize_tools.py

Just general tools.