# main.py
from fastapi import FastAPI
from clients.sunsynk import PlantAPI

app = FastAPI()
credentials = {"username": "solar@houss.co.za", "password": "Inverter@Houss"}
plants = PlantAPI(**credentials)

@app.get("/")
def root():
    return {"message": "API is running"}

@app.get("/plants")
def get_plants(page: int = 1):
    return plants.list(page=page)

if __name__ == "__main__":
    import uvicorn
   # print("Fetching plants...")
    #print(plants.list(page=1))
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
