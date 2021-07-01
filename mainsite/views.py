from django.shortcuts import render, redirect, HttpResponse
from django.contrib.auth.forms import AuthenticationForm, PasswordChangeForm
from django.contrib.auth import logout, login, authenticate, update_session_auth_hash
from django.contrib import messages
# Create your views here.


def landing(request):
    return render(request=request,
                  template_name="mainsite/homepage.html",
                  context={})


def login_request(request):
    form = AuthenticationForm()
    #IF this is a POST request, that means someone hit the SUBMIT button and we are accessing this def with data
    if request.method == "POST":
        form = AuthenticationForm(request, data=request.POST) # So if we're hitting this with a POST method, then our form is populated.
        if form.is_valid():
            username = form.cleaned_data.get('username')  # 'username' is the field name
            password = form.cleaned_data.get('password')  # 'password' is the field name
            user = authenticate(username=username, password=password)
            if user is not None:
                # Note: user is parameter in the login module that will be passed in the context of the the redirect page
                # (that way you can access things like {user.username}
                login(request, user=user)
                messages.success(request, f"You are now logged in as: {username}") #NOTE: f-string: we are now passing the variable {username} to the homepage
                # Redirect them to any page ("") will redirect them to the homepage
                # "main:homepage" goes into urls.py, looks for the app_name="main" and
                # then finds the link associated with name="homepage"
                return redirect("AbayDashboard:dash_django")
            else:
                messages.error(request, "Invalid username or password")
        else:
            messages.error(request, "Invalid username or password")
    form = AuthenticationForm()
    return render(request,
                  "AbayDashboard/login.html",
                  {"form": form})