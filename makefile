.SILENT: package

package:
	echo "=========================================="
	echo "= Bilding zip archive for cloud finction ="
	echo "==========================================\n"
	rm build/function_package.zip
	zip -r build/function_package.zip requirements.txt \
		tracker-import.py isoduration/* \
		-x *.pyc -x isoduration/__pycache__/\* \
		-x isoduration/formatter/__pycache__/\* \
		-x isoduration/operations/__pycache__/\* \
		-x isoduration/parser/__pycache__/\*