from module import inputs, optimize_profile, optimize_else, portfolio_return,secs,client_port
from fastapi import FastAPI
from typing import List, Dict
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import matplotlib.pyplot as plt
import numpy as np
import io
import base64

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class ProfileRequest(BaseModel):
    securities: List[str]
    target: str 
    
class ClientRequest(BaseModel):
    securities: List[str]
    weights: List[float]

# Глобальная переменная для хранения currency
global_currency = None

# Функция для оптимизации по инвестпрофилю
def o_profile(securities: List[str], target: str):
    global global_currency
    returns,cov_matrix,risk_free, daily_returns,df = inputs(securities, global_currency)
    data,quantity_df = optimize_profile(target,returns, cov_matrix,df)
    image = portfolio_return(data,daily_returns,df)
    return (data, quantity_df, image)

# Функция для оптимизации по различным критериям
def o_else(securities: List[str], criterion: str):
    global global_currency
    returns, cov_matrix, risk_free, daily_returns, df = inputs(securities, global_currency)
    data,quantity_df = optimize_else(criterion, returns, cov_matrix, risk_free,df)
    image = portfolio_return(data, daily_returns, df)
    return (data, quantity_df, image)

def g():
    t = np.arange(0.0, 2.0, 0.01)
    s = 1 + np.sin(2 * np.pi * t)

    plt.figure()
    plt.plot(t, s)

    plt.xlabel('time (s)')
    plt.ylabel('voltage (mV)')
    plt.title('About as simple as it gets, folks')
    plt.grid()

    # plt.savefig("test.png")

    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    image_bytes : bytes = base64.b64encode(buf.read()).decode('utf-8');
    buf.close()

    file = open("image.png", "wb")
    file.write(base64.b64decode(image_bytes))
    file.close()

@app.get("/securities")
def get_securities(currency: str):
    global global_currency
    data = secs(currency)
    global_currency = currency
    g()
    return data

class ProfileRespons(BaseModel):
    image: bytes
    data: List[Dict]
    quantity_df: List[Dict]

@app.post("/optimize/profile")
def opt_profile(request: ProfileRequest) -> ProfileRespons:
    (data, quantity_df, image) = o_profile(request.securities, request.target)
    return ProfileRespons(
        image=image,
        data=data.to_dict(orient="records"),
        quantity_df=quantity_df.to_dict(orient="records"))

@app.post("/optimize/else")
def opt_yield(request: ProfileRequest) -> ProfileRespons:
    global global_currency
    (data, quantity_df, image) = o_else(request.securities, request.target)
    return ProfileRespons(
        image=image,
        data= data.to_dict(orient="records"),
        quantity_df = quantity_df.to_dict(orient="records"))

class ClientRespons(BaseModel):
    image: bytes
    data: List[Dict]

@app.post("/optimize/client_port")
def opt_client(request: ClientRequest) -> ClientRespons:
    global global_currency
    (data, image) = client_port(request.securities,request.weights,global_currency)
    return ClientRespons(image=image, 
            data = data.to_dict(orient="records"))


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

#uvicorn main:app --reload