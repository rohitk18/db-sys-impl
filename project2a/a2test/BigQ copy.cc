#include "BigQ.h"
#include <unistd.h>


File file;
char *path = "temporary.bin";

std::vector<int> indexOfPages;
std::vector<RecordTracker *> sortRec;
std::vector<Page *> pagesOfRuns;
OrderMaker *order;
int atRun = 0;



RecordTracker::RecordTracker() {
	r = new Record();
	runCount = 0;
	numberOfPage = 0;
	
}


bool compareHeap(const RecordTracker *left, const RecordTracker *right)
{
  ComparisonEngine cEngine;

  Record *sRecord = right->r;
  Record *fRecord = left->r;
  

  if (cEngine.Compare(fRecord, sRecord, order) < 0) {
    return false;
  } 
  return true;
}




bool compareRecord(const RecordTracker *left, const RecordTracker *right) 
{
  ComparisonEngine cEngine;

  Record *sRecord = right->r;
  Record *fRecord = left->r;
  

  if (cEngine.Compare(fRecord, sRecord, order) < 0) {
    return true;
  }
   return false;

}



void mergingProcessOnPages(thread_p *tparameters)
{


  RecordTracker *currentRecord;
  RecordTracker *nextRecord;
  Page* pageTe = NULL;
  Record *varRecord;

  sortRec.clear();
  file.Open(1, path);

  	int i = 0;
	int currentRun = 0;
    int outgoingPage = 0;
	int sizeOfPageVec = indexOfPages.size();
	int no = 0;
	int varP = 0;
	
	if(sizeOfPageVec == 1){
		
		pageTe = new Page();
		while(file.GetPage(pageTe,i)){
			
			varRecord = new Record();
			while(pageTe->GetFirst(varRecord)) {
				
				
				tparameters->outP->Insert(varRecord);
				varRecord = new Record();	
				
			}
			pageTe->EmptyItOut();
			i++;
		}
			
	
	}
 else
 {
	while(i < sizeOfPageVec){
		
	pageTe = new Page();
    file.GetPage(pageTe,indexOfPages[i]);
    pagesOfRuns.push_back(pageTe);


    varRecord = new Record();
    pagesOfRuns[i]->GetFirst(varRecord);
    

    currentRecord = new RecordTracker();
    currentRecord->r = varRecord;
    currentRecord->runCount = i;
    currentRecord->numberOfPage = indexOfPages[i];

    sortRec.push_back(currentRecord);
		
		
		i++;
	}
 
 
 
  while(!sortRec.empty())
  {
	no++;

	std::make_heap(sortRec.begin(),sortRec.end(),compareHeap);
    

    currentRecord = new RecordTracker();
    currentRecord = sortRec.front(); 
    currentRun = currentRecord->runCount;
    outgoingPage = currentRecord->numberOfPage;

    std::pop_heap(sortRec.begin(),sortRec.end());
    sortRec.pop_back();


    tparameters->outP->Insert(currentRecord->r);

    nextRecord = new RecordTracker();
    nextRecord->runCount = currentRun;
    nextRecord->numberOfPage = outgoingPage;


    if(pagesOfRuns[currentRun]->GetFirst(nextRecord->r)) {
      sortRec.push_back(nextRecord);
      
    }
    else {
		
		if((currentRun+1) == (indexOfPages.size()))
		{
			varP = 1;
		}

      if(outgoingPage+2 < file.GetLength()){ 

        if((outgoingPage+1)< indexOfPages[currentRun+1] || varP==1) {
          
          pageTe = new Page();
          file.GetPage(pageTe,outgoingPage+1);
          pagesOfRuns[currentRun] = pageTe;
        
          if(pagesOfRuns[currentRun]->GetFirst(nextRecord->r)) {
            nextRecord->numberOfPage = outgoingPage+1;
            sortRec.push_back(nextRecord);
          
          }
          
        }
      } 
      
      varP = 0;

    }
  }
}
  

  file.Close();
   tparameters->outP->ShutDown();

}



void pagesToFile() 
{

	
	Page pageTe;
	Record *tempRec = NULL;


  int numberOfPage = 0;

  numberOfPage = file.GetLength();
  
  
  if(numberOfPage>0){
	  
	  numberOfPage = numberOfPage-1;
  }

  indexOfPages.push_back(numberOfPage);
  int i = 0;
  int size = sortRec.size();
  

	while(i < size) {
	
	
	 tempRec = sortRec[i]->r;


    if (!pageTe.Append(tempRec)) {


      file.AddPage(&pageTe, numberOfPage);


      pageTe.EmptyItOut();
      numberOfPage++;
	  pageTe.Append(tempRec);
    }
	
	i++;
	
}

  file.AddPage(&pageTe, numberOfPage);
 
  

}





void *eSorting(void *tparameters){
	
	
thread_p *td = (thread_p *) tparameters;

	int pagesCount = 0;
	bool pipeFilled = 1;
	
	Record fetchRecord;
	RecordTracker *tRec;
	Page pageTe;
	int no = 0;

//	cout<<"printing inside external sort of bigq"<<endl;



	file.Open(0, path);

	while (pipeFilled) {
		while (pagesCount < td->runLength) {

			if (td->inP->Remove(&fetchRecord)) {
				no++;

				tRec = new (std::nothrow) RecordTracker;
				tRec->r = new Record;

				tRec->r->Copy(&fetchRecord);
				tRec->runCount = atRun;


				sortRec.push_back(tRec);




				if (0 == pageTe.Append(&fetchRecord)) {

					pagesCount++;
					pageTe.EmptyItOut();
					pageTe.Append(&fetchRecord);

				}
			} else {

				pipeFilled = 0;
				
				break;
			}
		}


		atRun++;
		
		std::sort(sortRec.begin(), sortRec.end(), compareRecord);

		pagesToFile();


		
		pagesCount = 0;
	
		
		sortRec.clear();
	
	}
	 

	file.Close();

	mergingProcessOnPages(td);
}	






BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {



thread_p *thrd = new (std::nothrow) thread_p;



	thrd->inP = &in;
	thrd->outP = &out;
	thrd->sortOrder = &sortorder;
	thrd->runLength = runlen;
	order = &sortorder;

	sortorder.Print();
	

	pthread_t bThread;
	pthread_create(&bThread, NULL, eSorting, (void *) thrd);
}


	

BigQ::~BigQ () {
}


//mergePages: mergingProcessOnPages
//externalSort: eSorting
//transferPagesToFiles:  pagesToFile