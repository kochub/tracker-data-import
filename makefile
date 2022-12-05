.SILENT: package

package:
	echo "=========================================="
	echo "= Bilding zip archive for cloud finction ="
	echo "==========================================\n"
	zip -r build/function_package.zip requirements.txt tracker-import.py isoduration/*