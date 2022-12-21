.SILENT: package

package:
	echo "=========================================="
	echo "= Bilding zip archive for cloud finction ="
	echo "==========================================\n"
	rm build/function_package.zip
	zip -r build/function_package.zip requirements.txt tracker-import.py isoduration/*