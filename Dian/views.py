from django.shortcuts import render

# Create your views here.
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
import threading
import json

from .logicaSelenium.webScrapingDian import controlMCode
# from logicaSelenium.webScrapingDian import controlMCode
# from logicaSelenium.webScrapingDian import controlMCode

@csrf_exempt
# @require_POST
def ejecutar_scraping(request):
    # template_name = 'init.html'
    print("Request method:", request.method)
    if request.method == 'GET':
        return render(request, 'init.html')  # Si tu archivo está en Dian/templates/Dian/init.html
    elif request.method == 'POST':
        try:
            data = json.loads(request.body.decode('utf-8'))
            fechainicial = data.get('fechainicial')
            fechafinal = data.get('fechafinal')
            token = data.get('token')
            correo = data.get('correo')

            if not (fechainicial and fechafinal and token and correo):
                return JsonResponse({'error': 'Faltan parámetros'}, status=400)

            def run_scraping():
                # Instancia tu clase con los parámetros recibidos
                scraping = controlMCode(
                    proceso=1,
                    usuarioD=token,
                    passwordD='890927624',  # Si el password también es variable, pásalo desde el front
                    fechainicial=fechainicial,
                    fechafinal=fechafinal
                )
                # Sobrescribe el método de envío de correo para usar el correo recibido
                # def enviar_correo_personalizado(asunto, adjuntos=None, concopia=None):
                #     scraping.enviarCorreo(asunto, adjuntos, correo)
                # scraping.enviar_correoAwsSes = enviar_correo_personalizado
                scraping.EjecutarWeb(token=token, fechainicial=fechainicial, fechafinal=fechafinal, correo=correo)

            threading.Thread(target=run_scraping).start()
            # return JsonResponse({'status': 'ok', 'mensaje': 'Scraping iniciado'})
            return JsonResponse({'status': 'ok', 'mensaje': 'Scraping iniciado'})
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)
    else:
        return JsonResponse({'error': 'Método no permitido'}, status=405)