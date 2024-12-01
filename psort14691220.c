#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#define TAM_REGISTRO 100
#define TAM_CHAVE 4
#define TAM_DADOS 96

typedef struct {
    int chave;
    char dados[TAM_DADOS];
} Registro;

Registro *registros;
int qnt_registros;

void merge(Registro *arr, int inicio, int meio, int fim) {
    int n1 = meio - inicio + 1;
    int n2 = fim - meio;

    Registro *esq = (Registro *)malloc(n1 * sizeof(Registro));
    Registro *dir = (Registro *)malloc(n2 * sizeof(Registro));

    for (int i = 0; i < n1; i++)
        esq[i] = arr[inicio + i];
    for (int i = 0; i < n2; i++)
        dir[i] = arr[meio + 1 + i];

    int i = 0, j = 0, k = inicio;

    while (i < n1 && j < n2) {
        if (esq[i].chave <= dir[j].chave)
            arr[k++] = esq[i++];
        else
            arr[k++] = dir[j++];
    }

    while (i < n1)
        arr[k++] = esq[i++];

    while (j < n2)
        arr[k++] = dir[j++];

    free(esq);
    free(dir);
}

void merge_sort(Registro *arr, int inicio, int fim) {
    if (inicio < fim) {
        int meio = inicio + (fim - inicio) / 2;

        merge_sort(arr, inicio, meio);
        merge_sort(arr, meio + 1, fim);

        merge(arr, inicio, meio, fim);
    }
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Número inválido de argumentos!\n");
        return 1;
    }

    FILE* entrada = fopen(argv[1], "rb");
    if (!entrada) {
        perror("Erro ao abrir arquivo de entrada");
        return 1;
    }

    fseek(entrada, 0, SEEK_END);
    long tam_arq = ftell(entrada);
    fseek(entrada, 0, SEEK_SET);

    qnt_registros = tam_arq / TAM_REGISTRO;
    registros = (Registro *)malloc(tam_arq);

    if (fread(registros, TAM_REGISTRO, qnt_registros, entrada) != qnt_registros) {
        perror("Erro ao ler registros do arquivo de entrada");
        fclose(entrada);
        free(registros);
        return 1;
    }
    fclose(entrada);

    merge_sort(registros, 0, qnt_registros - 1);

    int fd = open(argv[2], O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        perror("Erro ao abrir arquivo de saída");
        free(registros);
        return 1;
    }

    if (write(fd, registros, tam_arq) != tam_arq) {
        perror("Erro ao escrever registros no arquivo de saída");
        close(fd);
        free(registros);
        return 1;
    }

    if (fsync(fd) == -1) {
        perror("Erro ao sincronizar arquivo de saída");
        close(fd);
        free(registros);
        return 1;
    }

    close(fd);
    free(registros);
    return 0;
}