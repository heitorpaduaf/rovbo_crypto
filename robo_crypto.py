import pandas as pd 
import os 
import time
from binance.client import Client
from binance.enums import *
import math

api_key = os.getenv("KEY_BINANCE")
secret_key = os.getenv("SECRET_BINANCE")

client_binance = Client(api_key, secret_key)

conta =  client_binance.get_account()

codigo_operado = "BTCBRL"
ativo_operado = "BTC"
periodo_candle = Client.KLINE_INTERVAL_1HOUR
balanceBRL = client_binance.get_asset_balance(asset="BRL")
balanceBTC = client_binance.get_asset_balance(asset="BTC")
BTC_disponivel = float(balanceBTC["free"])
BRL_disponivel = float(balanceBRL["free"])
info = client_binance.get_symbol_info("BTCBRL")
step_size = float([f["stepSize"] for f in info["filters"] if f["filterType"] == "LOT_SIZE"][0])
TAXA_BINANCE = 0.001
client_binance = Client(api_key, secret_key, {"timeout": 30})


def pegando_dados(codigo, intervalo):
    
    candles = client_binance.get_klines(symbol = codigo, interval = intervalo, limit = 1000)
    precos = pd.DataFrame(candles)
    precos.columns = ["tempo_abertura","abertura", "maxima","minima", "fechamento","volume","tempo_fechamento","moedas_negociadas", "numero_trades",
                   "volume_ativo_base_compra","volume_ativo_cotação","-"]
    precos = precos[["fechamento", "tempo_fechamento"]]
    precos["tempo_fechamento"] = pd.to_datetime(precos["tempo_fechamento"], unit= "ms").dt.tz_localize("UTC")
    precos["tempo_fechamento"] = precos["tempo_fechamento"].dt.tz_convert("America/Sao_Paulo")

    return precos

dados_atualizados = pegando_dados(codigo=codigo_operado, intervalo=periodo_candle)

def estrategia_trade(dados, BRL_disponivel, BTC_disponivel, posicao):

    dados["media_rapida"] = dados["fechamento"].rolling(window = 7).mean()
    dados["media_saida"] = dados["fechamento"].rolling(window = 15).mean()
    dados["media_devagar"] = dados["fechamento"].rolling(window = 35).mean()

    ultima_media_rapida = dados["media_rapida"].iloc[-1]
    ultima_media_devagar = dados["media_devagar"].iloc[-1]
    ultima_media_saida = dados["media_saida"].iloc[-1]

    print(f"Ultima Media Rápida: {ultima_media_rapida} | Ultima Media Saida: {ultima_media_saida} | Ultima Media Devagar: {ultima_media_devagar}")

    balanceBRL = client_binance.get_asset_balance(asset="BRL")
    balanceBTC = client_binance.get_asset_balance(asset="BTC")
    BTC_disponivel = float(balanceBTC["free"])
    BRL_disponivel = float(balanceBRL["free"])
    BTC_vender = ((BTC_disponivel * 10000)/10000) * (1- TAXA_BINANCE)
    
    conta = client_binance.get_account()
    if BRL_disponivel < 1000:
        posicao = True
        print(f'Posição = {posicao}')

    elif BRL_disponivel > 1000:
        posicao = False
        print(f'Posição = {posicao}') 
    
    if ultima_media_rapida > ultima_media_saida and ultima_media_rapida > ultima_media_devagar:
        if posicao == False:
            if BRL_disponivel > 15:
                order = client_binance.create_order( 
                    symbol ="BTCBRL",
                    side = SIDE_BUY,
                    type = ORDER_TYPE_MARKET,
                    quoteOrderQty  = BRL_disponivel)
            print("Comprou o ativo")
        posicao = True
        
    elif ultima_media_rapida < ultima_media_saida:
        if posicao == True:      
                order = client_binance.create_order(
                symbol="BTCBRL",
                side=SIDE_SELL,
                type=ORDER_TYPE_MARKET,
                quantity= round(BTC_vender, 5)
    )
                print(f'Posição = {posicao}')
        posicao = False
        
    balanceBRL = client_binance.get_asset_balance(asset="BRL")
    balanceBTC = client_binance.get_asset_balance(asset="BTC")
    BTC_disponivel = float(balanceBTC["free"])
    BRL_disponivel = float(balanceBRL["free"])  

    print(f'BRL disponivel = {BRL_disponivel}')
    print(f'BTC disponivel = {BTC_disponivel}')

    return posicao

posicao_atual = True

while True:

        dados_atualizados =pegando_dados(codigo=codigo_operado, intervalo=periodo_candle)
        posicao_atual =estrategia_trade(dados_atualizados, BRL_disponivel= BRL_disponivel,
        BTC_disponivel=BTC_disponivel, posicao= posicao_atual)
        time.sleep(1800) 