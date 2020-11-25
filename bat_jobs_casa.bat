:inicio
	taskkill /F /IM python.exe
	cd C:\@work\pessoal\Criptomante_python
	python job.py
	timeout /t 300
	goto :inicio