def contar_palabras(archivo):
    contador = 0
    buffer_anterior = ""
    chunk_size = 1024 * 1024  
    indice_invertido = {}  
    
    with open(archivo, 'r', encoding='utf-8') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                if buffer_anterior:
                    palabras_finales = buffer_anterior.split()
                    contador += len(palabras_finales)
                    for palabra in palabras_finales:
                        palabra_limpia = palabra.lower().strip('.,!?";:()[]{}')
                        if palabra_limpia:
                            indice_invertido[palabra_limpia] = indice_invertido.get(palabra_limpia, 0) + 1
                break
            
            texto_completo = buffer_anterior + chunk
            palabras = texto_completo.split()
            
            if len(palabras) > 1:
                palabras_completas = palabras[:-1]
                contador += len(palabras_completas)
                for palabra in palabras_completas:
                    palabra_limpia = palabra.lower().strip('.,!?";:()[]{}')
                    if palabra_limpia:
                        indice_invertido[palabra_limpia] = indice_invertido.get(palabra_limpia, 0) + 1
                buffer_anterior = palabras[-1]
            else:
                buffer_anterior = texto_completo
    
    return contador, indice_invertido

if __name__ == "__main__":
    archivo = r"D:\Codigos\TopicosPrimeraTarea\wikipedia.txt"
    total_palabras, indice = contar_palabras(archivo)
    print(f"El archivo tiene {total_palabras} palabras.")
    print(f"Palabras únicas: {len(indice)}")
    
    palabras_frecuentes = sorted(indice.items(), key=lambda x: x[1], reverse=True)
    print("\nTodas las palabras y su frecuencia:")
    for palabra, frecuencia in palabras_frecuentes:
        print(f"{palabra}: {frecuencia}")


# tiempo de 1412.787 segundos 24 minutos