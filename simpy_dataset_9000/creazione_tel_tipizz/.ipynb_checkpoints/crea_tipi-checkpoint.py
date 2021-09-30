import pandas as pd

ifile = pd.read_csv("sorted_tel_cod.csv")
finestra = 1800

out=open("typed_events.csv","w")

for i in range(len(ifile)):
    tmp = ifile.iloc[[i]].values.flatten().tolist()
    if tmp[1][-1] != 'S':
        continue  # l'evento base deve essere una chiamata ricevuta da un intercettato
    if i>0:
        tmp0 = ifile.iloc[[i-1]].values.flatten().tolist()
        if tmp0[0] == tmp[0] and tmp0[1] == tmp[1] and tmp0[2] == tmp[2]:
        # telefonate registrate due volte
            continue
    tempo = tmp[0]
    k = i + 1
    while True:
        if k>=len(ifile):
            break
        tmp1 = ifile.iloc[[k]].values.flatten().tolist()
        if tmp1[0] > tempo + finestra:
            break
        if tmp1[0] == tmp[0] and tmp1[1] == tmp[1] and tmp1[2] == tmp[2]:
            k=k+1
            continue  # telefonate registrate due volte
        if k>i+1:
            tmp0 = ifile.iloc[[k-1]].values.flatten().tolist()
            if tmp0[0] == tmp1[0] and tmp0[1] == tmp1[1] and tmp0[2] == tmp1[2]:
            # telefonate registrate due volte
                k = k + 1
                continue
        if tmp[2] == tmp1[1]:
            if tmp1[2] == tmp[1]:
            # richiama chi lo ha chiamato: non interessa
                k=k+1
                continue
        # il chiamato dell'evento base chiama qualcuno
            if tmp1[2][-1] == 'S' and tmp[1][-1] == 'S':
            # chiamato da un intercettato, chiama un intercettato, evento di tipo 1
                out.write('"E1",'+str(tempo)+',"'+tmp[1]+'","'+tmp[2]+'","'+tmp1[2]+'"\r\n')
#                print("E1")
            elif tmp1[2][-1] == 'S' and tmp[1][-1] != 'S':
            # chiamato da un NON intercettato, chiama un intercettato, evento di tipo 1
                out.write('"E2",'+str(tempo)+',"'+tmp[1]+'","'+tmp[2]+'","'+tmp1[2]+'"\r\n')
#                print("E2")
            elif tmp1[2][-1] == 'N' and tmp[1][-1] == 'S':
            # chiamato da un intercettato, chiama un NON intercettato, controllo se costui chiama un intercettato
                tempo1 = tmp1[0]
                j = k + 1
                while True:
                    if j > len(ifile):
                        break
                    tmp2 =  ifile.iloc[[j]].values.flatten().tolist()
                    if tmp2[0] > tempo1 + finestra:
                        break
                    if tmp2[0] == tmp1[0] and tmp2[1] == tmp1[1] and tmp2[2] == tmp1[2]:
                        j=j+1
                        continue  # telefonate registrate due volte
                    if tmp1[2] == tmp2[1]:
                        if tmp2[2][-1] == 'S':
                        # il chiamato dell'evento trovato chiama un intercettato: evento tipo 3 
                        #  siccome il chiamato non e' intercettato, quello che chiama dovrebbe essere per forza un intercettato
                        #  altrimenti non avremmo la telefonata, quindi inserisco il controllo, probabilmente e' sfuggito un numero intercettato
                            out.write('"E3",'+str(tempo)+',"'+tmp[1]+'","'+tmp[2]+'","'+tmp1[2]+'","'+tmp2[2]+'"\r\n')
#                            print("E3")
                        else:
                            print("Non risulta intercettato "+tmp2[2]+" controllare")
                            out.write('"E3",'+str(tempo)+',"'+tmp[1]+'","'+tmp[2]+'","'+tmp1[2]+'","'+tmp2[2]+'"\r\n')
                    j=j+1
#                    print("j",j)
            elif tmp1[2][-1] == 'N' and tmp[1][-1] == 'N':
            # chiamato da un NON intercettato, chiama un NON intercettato, controllo se costui chiama un intercettato
                tempo1 = tmp1[0]
                j = k + 1
                while True:
                    if j > len(ifile):
                        break
                    tmp2 =  ifile.iloc[[j]].values.flatten().tolist()
                    if tmp2[0] > tempo1 + finestra:
                        break
                    if tmp2[0] == tmp1[0] and tmp2[1] == tmp1[1] and tmp2[2] == tmp1[2]:
                        j=j+1
                        continue  # telefonate registrate due volte
                    if tmp1[2] == tmp2[1]:
                        if tmp2[2][-1] == 'S':
                        # il chiamato dell'evento trovato chiama un intercettato: evento tipo 3 
                        #  siccome il chiamato non e' intercettato, quello che chiama dovrebbe essere per forza un intercettato
                        #  altrimenti non avremmo la telefonata, quindi inserisco il controllo, probabilmente e' sfuggito un numero intercettato
                            out.write('"E4",'+str(tempo)+',"'+tmp[1]+'","'+tmp[2]+'","'+tmp1[2]+'","'+tmp2[2]+'"\r\n')
#                            print("E4")
                        else:
                            print("Non risulta intercettato "+tmp2[2]+" controllare")
                            out.write('"E4",'+str(tempo)+',"'+tmp[1]+'","'+tmp[2]+'","'+tmp1[2]+'","'+tmp2[2]+'"\r\n')
                    j=j+1
#                    print("j",j)
        k = k + 1
#        print("k",k)
        
    if i % 500 == 0:
        print("i",i)

out.close()