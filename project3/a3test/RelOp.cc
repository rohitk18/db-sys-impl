#include "RelOp.h"

/* SelectFile Class methods */
void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal)
{
	this->inFile = &inFile;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create(&thread, NULL, operationHelper, (void *)this);
}

void *SelectFile::operationHelper(void *args)
{
	((SelectFile *)args)->operation();
}

void *SelectFile::operation()
{
	Record record;
	inFile->MoveFirst();
	ComparisonEngine ce;
	int i = 0;
	while (inFile->GetNext(record))
	{
		if (selOp != NULL)
		{
			if (ce.Compare(&record, literal, selOp))
			{
				outPipe->Insert(&record);
			}
		}
		else
		{
			outPipe->Insert(&record);
		}
	}
	outPipe->ShutDown();
}

void SelectFile::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void SelectFile::Use_n_Pages(int n)
{
	this->nPages = n;
}

/* SelectPipe class methods */

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal)
{
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create(&thread, NULL, operationHelper, (void *)this);
}

void *SelectPipe::operationHelper(void *args)
{
	((SelectPipe *)args)->operation();
}

void *SelectPipe::operation()
{
	Record record;
	ComparisonEngine ce;
	while (inPipe->Remove(&record))
	{
		if (selOp != NULL)
		{
			if (ce.Compare(&record, literal, selOp))
			{
				outPipe->Insert(&record);
			}
		}
		else
		{
			outPipe->Insert(&record);
		}
	}
	outPipe->ShutDown();
}

void SelectPipe::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void SelectPipe::Use_n_Pages(int n)
{
	this->nPages = n;
}

/* Project Class methods */

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput)
{
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->keepMe = keepMe;
	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;
	pthread_create(&thread, NULL, operationHelper, (void *)this);
}

void *Project::operationHelper(void *args)
{
	((Project *)args)->operation();
}

void *Project::operation()
{
	Record record;
	int i = 0;
	while (inPipe->Remove(&record))
	{
		record.Project(keepMe, numAttsOutput, numAttsInput);
		outPipe->Insert(&record);
		i++;
	}
	inPipe->ShutDown();
	outPipe->ShutDown();
}

void Project::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void Project::Use_n_Pages(int n)
{
	this->nPages = n;
}

/* Join Class methods */

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)
{
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	this->nPages = nPages ? nPages : 1;
	pthread_create(&thread, NULL, operationHelper, (void *)this);
}

void *Join::operationHelper(void *args)
{
	((Join *)args)->operation();
}
void *runBigQ(void *arg)
{
	BigQArgs *t = (BigQArgs *)arg;
	BigQ bigQ(*(t->in), *(t->out), *(t->sortorder), t->runlen);
}
void *Join::operation()
{
	OrderMaker lSortOrder, rSortOrder;
	int flagMergeSort;
	flagMergeSort = selOp->GetSortOrders(lSortOrder, rSortOrder);
	ComparisonEngine ce;
	int joinFinish = 0; // flag for finishing join
	int numAttsLeft, numAttsRight, numAttsToKeep, startRight;
	int *attsToKeep;
	if (flagMergeSort)
	{
		// using merge sort
		Record lRecord, rRecord;
		Pipe *outPipeL;
		Pipe *outPipeR;
		outPipeL = new (std::nothrow) Pipe(100);
		outPipeR = new (std::nothrow) Pipe(100);
		BigQArgs *leftArgs = new (std::nothrow) BigQArgs();
		leftArgs->in = inPipeL;
		leftArgs->out = outPipeL;
		leftArgs->runlen = nPages;
		leftArgs->sortorder = &lSortOrder;

		BigQArgs *rightArgs = new (std::nothrow) BigQArgs();
		rightArgs->in = inPipeR;
		rightArgs->out = outPipeR;
		rightArgs->runlen = nPages;
		rightArgs->sortorder = &rSortOrder;

		pthread_t lthread, rthread;
		pthread_create(&lthread, NULL, runBigQ, (void *)leftArgs);
		pthread_create(&rthread, NULL, runBigQ, (void *)rightArgs);

		int loPipeStatus = outPipeL->Remove(&lRecord);
		int roPipeStatus = outPipeR->Remove(&rRecord);

		Record mergedRecord;

		while (true)
		{
			if (loPipeStatus && roPipeStatus)
			{
				if (ce.Compare(&lRecord, &lSortOrder, &rRecord, &rSortOrder) < 0)
				{
					loPipeStatus = outPipeL->Remove(&lRecord);
					continue;
				}
				else if (ce.Compare(&lRecord, &lSortOrder, &rRecord, &rSortOrder) > 0)
				{
					roPipeStatus = outPipeR->Remove(&rRecord);
					continue;
				}
				else
				{
					//Compute cross product
					vector<Record *> recordsVector;
					Record *tempRecord = new (std::nothrow) Record();
					tempRecord->Consume(&rRecord);

					while (roPipeStatus && !ce.Compare(&lRecord, &lSortOrder, tempRecord, &rSortOrder))
					{
						recordsVector.push_back(tempRecord);
						tempRecord = new (std::nothrow) Record();
						roPipeStatus = outPipeR->Remove(tempRecord);
					}
					if (roPipeStatus)
						rRecord.Consume(tempRecord);

					delete tempRecord;
					tempRecord = NULL;

					while (loPipeStatus && !ce.Compare(&lRecord, &lSortOrder, recordsVector.front(), &rSortOrder))
					{

						for (int i = 0; i < recordsVector.size(); i++)
						{
							if (ce.Compare(&lRecord, recordsVector[i], literal, selOp))
							{
								if (!joinFinish)
								{
									numAttsLeft = ((((int *)(lRecord.bits))[1]) / sizeof(int)) - 1;
									numAttsRight = ((((int *)(recordsVector[i]->bits))[1]) / sizeof(int)) - 1;
									numAttsToKeep = numAttsLeft + numAttsRight;

									attsToKeep = new (std::nothrow) int[numAttsToKeep];

									int k;
									for (k = 0; k < numAttsLeft; k++)
									{
										attsToKeep[k] = k;
									}
									startRight = k;
									for (int l = 0; l < numAttsRight;
										 l++, k++)
									{
										attsToKeep[k] = l;
									}
									joinFinish = 1;
								}

								mergedRecord.MergeRecords(&lRecord, recordsVector[i], numAttsLeft, numAttsRight, attsToKeep,numAttsToKeep, startRight);
								outPipe->Insert(&mergedRecord);
							}
						}
						loPipeStatus = outPipeL->Remove(&lRecord);
					}

					// deleting vector memory of records
					for (int i = 0; i < recordsVector.size(); i++)
					{
						delete recordsVector[i];
					}
					recordsVector.empty();
				}
			}
			else
			{
				break;
			}
		}
		delete outPipeL;
		delete outPipeR;
		delete attsToKeep;
	}
	else
	{
		//Block Nested Loop Join
		Page page;
		int count = 0;
		vector<Record *> lRecordsVector;
		vector<Record *> rRecordsVector;
		int lPipeStatus = 0, rPipeStatus = 0;
		Record lRecord, rRecord;
		Record *tempRecord;
		File f;
		f.Open(0, "temp.bin");
		int fileReady = 0;
		int currentPage = 0;
		int scanPtr = 0;
		int cnt = 0;
		while (true)
		{
			if ((lPipeStatus = inPipeL->Remove(&lRecord)) && count < (nPages - 1))
			{
				if (!page.Append(&lRecord))
				{
					tempRecord = new (std::nothrow) Record();
					while (page.GetFirst(tempRecord))
					{
						lRecordsVector.push_back(tempRecord);
						tempRecord = new Record();
					}

					delete tempRecord;
					tempRecord = NULL;
					page.Append(&lRecord);
					count++;
				}
			}
			else
			{
				cnt += lRecordsVector.size();
				int scanEnd = 0, scanPtr = 0;
				Page scanPage;
				if (count < nPages - 1)
				{
					tempRecord = new (std::nothrow) Record();
					while (page.GetFirst(tempRecord))
					{
						lRecordsVector.push_back(tempRecord);
						tempRecord = new (std::nothrow) Record();
					}
					delete tempRecord;
					tempRecord = NULL;
				}

				while (true)
				{
					if (!fileReady)
					{
						Page tempPage;
						Record *tempRecord1;
						tempRecord = new (std::nothrow) Record();
						tempRecord1 = new (std::nothrow) Record();
						while (scanPage.GetFirst(tempRecord))
						{
							tempRecord1->Copy(tempRecord);
							rRecordsVector.push_back(tempRecord);
							tempPage.Append(tempRecord1);
							tempRecord = new (std::nothrow) Record();
							tempRecord1 = new (std::nothrow) Record();
						}
						delete tempRecord;
						delete tempRecord1;
						tempRecord = NULL;
						tempRecord1 = NULL;

						f.AddPage(&tempPage, currentPage++);
						tempPage.EmptyItOut();
						scanPage.EmptyItOut(); //
					}
					else
					{
						if (scanPtr < f.GetLength() - 1)
						{
							f.GetPage(&scanPage, scanPtr++);
							tempRecord = new (std::nothrow) Record();
							while (scanPage.GetFirst(tempRecord))
							{
								rRecordsVector.push_back(tempRecord);
								tempRecord = new (std::nothrow) Record();
							}
							delete tempRecord;
							tempRecord = NULL;
							scanPage.EmptyItOut();
						}
						else
							scanEnd = 1;
					}

					cnt += rRecordsVector.size();

					for (int i = 0; i < lRecordsVector.size(); i++)
					{
						for (int j = 0; j < rRecordsVector.size(); j++)
						{
							if (ce.Compare(lRecordsVector[i], rRecordsVector[j],
										   literal, selOp))
							{
								Record mergedRecord;
								if (!joinFinish)
								{
									numAttsLeft =
										((((int *)(lRecordsVector[i]->bits))[1]) / sizeof(int)) - 1;
									numAttsRight =
										((((int *)(rRecordsVector[j]->bits))[1]) / sizeof(int)) - 1;
									numAttsToKeep = numAttsLeft + numAttsRight;

									attsToKeep =
										new (std::nothrow) int[numAttsToKeep];
									int k;
									for (k = 0; k < numAttsLeft; k++)
									{
										attsToKeep[k] = k;
									}

									startRight = k;

									for (int l = 0; l < numAttsRight;
										 l++, k++)
									{
										attsToKeep[k] = l;
									}
									joinFinish = 1;
								}

								mergedRecord.MergeRecords(lRecordsVector[i],
														  rRecordsVector[j], numAttsLeft,
														  numAttsRight, attsToKeep, numAttsToKeep,
														  startRight);
								outPipe->Insert(&mergedRecord);
							}
						}
						//delete lRecordsVector[i];
						//lRecordsVector[i]=NULL;
					}

					for (int i = 0; i < rRecordsVector.size(); i++)
					{
						delete rRecordsVector[i];
						rRecordsVector[i] = NULL;
					}
					rRecordsVector.clear();

					if (!rPipeStatus && !fileReady)
					{
						fileReady = 1;
						break;
					}
					else if (!fileReady)
					{
						scanPage.Append(&rRecord);
					}

					if (scanEnd)
					{
						//scanPtr=0;
						break;
					}
				}

				for (int i = 0; i < lRecordsVector.size(); i++)
				{
					delete lRecordsVector[i];
					lRecordsVector[i] = NULL;
				}

				lRecordsVector.clear();

				if (count >= (nPages - 1))
				{
					page.Append(&lRecord);
					cnt++;
					count = 0;
				}
				else
					break;

			}
		}

		f.Close();			//temp file close
		remove("temp.bin"); //temp file delete
	}

	outPipe->ShutDown();
}

void Join::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void Join::Use_n_Pages(int n)
{
	this->nPages = n;
}

/* DuplicateRemoval Class methods */

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->mySchema = &mySchema;
	pthread_create(&thread, NULL, operationHelper, (void *)this);
}

void *DuplicateRemoval::operationHelper(void *args)
{
	((DuplicateRemoval *)args)->operation();
}

void *DuplicateRemoval::operation()
{
	Record record;
	Record previous;
	ComparisonEngine ce;

	OrderMaker myOrder(mySchema);

	Pipe *in;
	Pipe *out;
	BigQArgs *args;
	pthread_t bigq_thread;
	in = new (std::nothrow) Pipe(100);
	out = new (std::nothrow) Pipe(100);
	args = new BigQArgs();
	args->in = in;
	args->out = out;
	args->sortorder = &myOrder;
	args->runlen = 1;
	pthread_create(&bigq_thread, NULL, runBigQ, (void *)args);

	//Write all the records to BigQ first
	//Get the records from outPipe and then compare with the previously fetched records
	//If it is same, then don't write that record to outPipe
	while (inPipe->Remove(&record))
	{
		in->Insert(&record);
	}

	in->ShutDown();
	bool firstIter = true;
	while (out->Remove(&record))
	{
		if (firstIter)
		{
			//No Previous record to comapare with. Store current as previous for next iteration
			previous.Copy(&record);
			firstIter = false;
		}
		else
		{
			if (!ce.Compare(&record, &previous, &myOrder))
			{
				//Prev Record same as current
				//Don't write to outPipe, No need to copy rec to previous
			}
			else
			{
				outPipe->Insert(&previous);
				previous.Copy(&record);
			}
		}
	}
	outPipe->ShutDown();
	pthread_join(bigq_thread, NULL);
}

void DuplicateRemoval::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int n)
{
	this->nPages = n;
}

/* Sum Class methods */

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->computeMe = &computeMe;
	pthread_create(&thread, NULL, operationHelper, (void *)this);
}

void *Sum::operationHelper(void *args)
{
	((Sum *)args)->operation();
}

void *Sum::operation()
{
	int intResult;
	double doubleResult;
	Record record;
	Type resultType;
	int sumInt = 0;
	double sumDouble = 0;
	while (inPipe->Remove(&record))
	{
		resultType = computeMe->Apply(record, intResult, doubleResult);
		if (resultType == Int)
		{
			sumInt = sumInt + intResult;
		}
		//Can each iteration can lead to Double / Int or Only Dbl or Only Int?
		else if (resultType == Double)
		{
			sumDouble = sumDouble + doubleResult;
		}
	}
	// can the return type be String?
	// create temperory schema, with one attribute - sum
	Record *resultRecord = new Record();
	if (resultType == Int)
	{
		Attribute attribute = {(char *)"sum", Int};
		Schema sumSchema((char *)"sumschema_file_unused", 1, &attribute);
		char sumString[30];
		sprintf(sumString, "%f|", sumInt);
		resultRecord->ComposeRecord(&sumSchema, sumString);
		resultRecord->Print(&sumSchema);
	}
	else if (resultType == Double)
	{
		Attribute attribute = {(char *)"sum", Double};
		Schema sumSchema((char *)"sumscheme_file_unused", 1, &attribute);
		char sumString[30];
		sprintf(sumString, "%f|", sumDouble);
		resultRecord->ComposeRecord(&sumSchema, sumString);
		resultRecord->Print(&sumSchema);
	}
	outPipe->Insert(resultRecord);
	outPipe->ShutDown();
}

void Sum::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void Sum::Use_n_Pages(int n)
{
	this->nPages = n;
}

/* GroupBy Class methods */

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->computeMe = &computeMe;
	pthread_create(&thread, NULL, operationHelper, (void *)this);
}

void *GroupBy::operationHelper(void *args)
{
	((GroupBy *)args)->operation();
}

void *GroupBy::operation()
{
	Pipe sortedOutPipe(1000);
	BigQ bigQ(*inPipe, sortedOutPipe, *groupAtts, nPages);

	Record *groupedRecord = new Record();

	int intResult;
	double doubleResult;
	Record *current = NULL;
	Record *previous = NULL;
	Type resultType;
	int sumInt;
	double sumDouble;

	ComparisonEngine ce;
	bool groupSwitch = false;
	bool pipeEmpty = false;

	previous = new Record();
	if (!sortedOutPipe.Remove(previous))
	{
		outPipe->ShutDown();
		return 0;
	}

	while (!pipeEmpty)
	{
		sumInt = 0;
		sumDouble = 0;
		intResult = 0;
		doubleResult = 0;
		current = new Record();
		groupSwitch = false;

		while (!pipeEmpty && !groupSwitch)
		{
			sortedOutPipe.Remove(current);
			if (current->bits != NULL)
			{
				if (ce.Compare(current, previous, groupAtts) != 0)
				{
					groupedRecord->Copy(previous);
					groupSwitch = true;
				}
			}
			else
				pipeEmpty = true;

			resultType = computeMe->Apply(*previous, intResult, doubleResult);
			if (resultType == Int)
			{
				sumInt = sumInt + intResult;
			}
			else if (resultType == Double)
			{
				sumDouble = sumDouble + doubleResult;
			}
			previous->Consume(current);
		}

		Record *sumRecord = new Record();
		if (resultType == Int)
		{
			Attribute attribute = {(char *)"sum", Int};
			Schema sumSchema((char *)"sumschema_file_unused", 1, &attribute);
			char sumString[30];
			sprintf(sumString, "%d|", sumInt);
			sumRecord->ComposeRecord(&sumSchema, sumString);
		}
		else if (resultType == Double)
		{
			Attribute attribute = {(char *)"sum", Double};
			Schema sumSchema((char *)"sumscheme_file_unused", 1, &attribute);
			char sumString[30];
			sprintf(sumString, "%f|", sumDouble);
			sumRecord->ComposeRecord(&sumSchema, sumString);
		}
		Record resultRecord;
		int sumAttsLen = groupAtts->numAtts + 1;
		int sumAtts[sumAttsLen];
		sumAtts[0] = 0;
		for (int i = 1; i < sumAttsLen; i++)
		{
			sumAtts[i] = groupAtts->whichAtts[i - 1];
		}
		resultRecord.MergeRecords(sumRecord, groupedRecord, 1, sumAttsLen - 1, sumAtts, sumAttsLen, 1);
		outPipe->Insert(&resultRecord);
	}

	outPipe->ShutDown();
}

void GroupBy::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void GroupBy::Use_n_Pages(int n)
{
	this->nPages = n;
}

/* WriteOut Class methods */

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema)
{
	this->inPipe = &inPipe;
	this->outFile = outFile;
	this->mySchema = &mySchema;
	pthread_create(&thread, NULL, operationHelper, (void *)this);
}

void *WriteOut::operationHelper(void *args)
{
	((WriteOut *)args)->operation();
}

void *WriteOut::operation()
{
	Record temp;
	long cnt = 0;
	// loop through all of the attributes
	int n = mySchema->GetNumAtts();
	Attribute *atts = mySchema->GetAtts();
	while (inPipe->Remove(&temp))
	{
		cnt++;
		// loop through all of the attributes
		for (int i = 0; i < n; i++)
		{
			// print the attribute name
			fprintf(outFile, "%s: ", atts[i].name);
			// use the i^th slot at the head of the record to get the
			// offset to the correct attribute in the record
			int pointer = ((int *)temp.bits)[i + 1];

			// here we determine the type, which given in the schema;
			// depending on the type we then print out the contents
			fprintf(outFile, "%c", '[');
			// first is integer
			if (atts[i].myType == Int)
			{
				int *myInt = (int *)&(temp.bits[pointer]);
				fprintf(outFile, "%d", *myInt);

				// then is a double
			}
			else if (atts[i].myType == Double)
			{
				double *myDouble = (double *)&(temp.bits[pointer]);
				fprintf(outFile, "%f", *myDouble);
				// then is a character string
			}
			else if (atts[i].myType == String)
			{
				char *myString = (char *)&(temp.bits[pointer]);
				fprintf(outFile, "%s", myString);
			}

			fprintf(outFile, "%c", ']');

			// print out a comma as needed to make things pretty
			if (i != n - 1)
			{
				fprintf(outFile, "%c", ',');
			}
		}
		fprintf(outFile, "%c", '\n');
	}
	fclose(outFile);
	cout << "\nRecords count:" << cnt << "\n";
}

void WriteOut::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void WriteOut::Use_n_Pages(int n)
{
	this->nPages = n;
}