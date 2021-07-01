from django.shortcuts import render
from django.http import HttpResponse
# Create your views here.


def plotly_snodas(request):
    return render(request=request, template_name='snodas/basin_snow.html')
