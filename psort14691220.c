#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

#define TAM_REGISTRO 100
#define TAM_CHAVE 4
#define TAM_DADOS 96

typedef struct {
    int chave;
    char dados[TAM_DADOS];
} Registro;

typedef struct {
    int inicio;
    int fim;
} ThreadArgs;

Registro *registros;
int qnt_registros;
int num_threads;

void merge(int inicio, int meio, int fim);
void* thread_merge_sort(void* arg);
void merge_sort(int inicio, int fim);

void merge(int inicio, int meio, int fim) {
    int n1 = meio - inicio + 1;
    int n2 = fim - meio;

    Registro *esq = (Registro *)malloc(n1 * sizeof(Registro));
    Registro *dir = (Registro *)malloc(n2 * sizeof(Registro));

    int i;

    for (i = 0; i < n1; i++)
        esq[i] = registros[inicio + i];
    for (i = 0; i < n2; i++)
        dir[i] = registros[meio + 1 + i];

    i = 0;
    int j = 0;
    int k = inicio;

    while (i < n1 && j < n2) {
        if (esq[i].chave <= dir[j].chave)
            registros[k++] = esq[i++];
        else
            registros[k++] = dir[j++];
    }

    while (i < n1)
        registros[k++] = esq[i++];

    while (j < n2)
        registros[k++] = dir[j++];

    free(esq);
    free(dir);
}

void* thread_merge_sort(void* arg) {
    ThreadArgs* args = (ThreadArgs*)arg;
    int inicio = args->inicio;
    int fim = args->fim;

    if (inicio < fim) {
        int meio = inicio + (fim - inicio) / 2;

        merge_sort(inicio, meio);
        merge_sort(meio + 1, fim);

        merge(inicio, meio, fim);
    }
    return NULL;
}

void merge_sort(int inicio, int fim) {
    if (inicio < fim) {
        int meio = inicio + (fim - inicio) / 2;

        merge_sort(inicio, meio);
        merge_sort(meio + 1, fim);

        merge(inicio, meio, fim);
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

    num_threads = atoi(argv[3]);
    if (num_threads <= 0) {
        num_threads = sysconf(_SC_NPROCESSORS_ONLN);
    }

    pthread_t threads[num_threads];
    ThreadArgs args[num_threads];
    int tamanho_por_thread = qnt_registros / num_threads;
    int i; 

    for (i = 0; i < num_threads; i++) {
        args[i].inicio = i * tamanho_por_thread;
        if (i != num_threads - 1) {
            args[i].fim = (i + 1) * tamanho_por_thread - 1;
        } else {
            args[i].fim = qnt_registros - 1;
        }
        pthread_create(&threads[i], NULL, thread_merge_sort, &args[i]);
    }

    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    for (i = 1; i < num_threads; i++) {
        int meio = (i * tamanho_por_thread) - 1;
        int fim;
        if (i != num_threads - 1) {
            fim = (i + 1) * tamanho_por_thread - 1;
        } else {
            fim = qnt_registros - 1;
        }
        merge(0, meio, fim);
    }

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