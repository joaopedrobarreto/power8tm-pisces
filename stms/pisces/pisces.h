/* =============================================================================
 *
 * pisces.h
 *
 * Implementation of the Pisces STM algorithm, according to the pseudo-code
 * presented in the paper:
 * "Pisces: A Scalable and Efficient Persistent Transactional Memory" (ATC'19)
 *
 * =============================================================================
 *
 * Contributors: João Pedro Barreto (joao.barreto@tecnico.ulisboa.pt)
 *
 * =============================================================================
 */


#ifndef pisces_H
#define pisces_H 1




#include <pthread.h>


//extern pthread_mutex_t cas_mutex;






#include <stdint.h>


#  include <setjmp.h>




# ifndef MIN_BACKOFF
#  define MIN_BACKOFF                   (1UL << 2)
# endif /* MIN_BACKOFF */
# ifndef MAX_BACKOFF
#  define MAX_BACKOFF                   (1UL << 31)
# endif /* MAX_BACKOFF */

typedef struct _Thread Thread;

#ifdef __cplusplus
extern "C" {
#endif





#  include <setjmp.h>
#  define SIGSETJMP(env, savesigs)      sigsetjmp(env, savesigs)
#  define SIGLONGJMP(env, val)          siglongjmp(env, val); assert(0)



/*
 * Prototypes
 */

//extern __thread unsigned int local_exec_mode;
//extern __thread void* rot_readset[];
//extern __thread unsigned long rs_counter;


void     TxStart       (Thread*, sigjmp_buf*);

Thread*  TxNewThread   ();





void     TxFreeThread  (Thread*);
void     TxInitThread  (Thread*, long id);
int      TxCommit      (Thread*);
int      TxCommitSTM   (Thread*);
void     TxAbort       (Thread*);
intptr_t TxLoad        (Thread*, volatile intptr_t*);
void     TxStore       (Thread*, volatile intptr_t*, intptr_t);
void     TxStoreLocal  (Thread*, volatile intptr_t*, intptr_t);
void     TxOnce        ();
void     TxShutdown    ();

void*    TxAlloc       (Thread*, size_t);
void     TxFree        (Thread*, void*);

void     TxIncClock    ();
long	 TxReadClock   (); 

long     TxValidate    (Thread*);
long     TxFinalize    (Thread*, long);
void     TxResetAfterFinalize (Thread*);




#ifdef __cplusplus
}
#endif


#endif /* pisces_H */


/* =============================================================================
 *
 * End of pisces.h
 *
 * =============================================================================
 */
