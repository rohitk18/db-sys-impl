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
	cout << "working project run" << endl;
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
	cout << "i value " << i << endl;
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
	// pthread_create(thread, NULL, operationHelper, (void *)this);
}

void *Join::operationHelper(void *args)
{
	((Join *)args)->operation();
}
void *run_bigq(void *arg)
{

	bigq_util *t = (bigq_util *)arg;
	BigQ b_queue(*(t->in), *(t->out), *(t->sort_order), t->run_len);
}
void *Join::operation() 	
{/*
	OrderMaker left, right;
	int use_sort_merge;
	use_sort_merge = selOp->GetSortOrders(left, right);
	ComparisonEngine comp;
	//	cout << "use merge " << use_sort_merge << endl;
	int join_calc_done = 0, numAttsLeft, numAttsRight, numAttsToKeep,
		startOfRight;
	int *attsToKeep;
	//Sort Merge Join
	//use_sort_merge=0;
	if (use_sort_merge)
	{
		Record rec1, rec2;
		//cout<<"sm1"<<endl;
		//inPipeL->Remove(&rec1);
		//inPipeR->Remove(&rec2);

		//cout<<"sm2"<<endl;
		Pipe *PipeL;
		Pipe *PipeR;
		PipeL = new (std::nothrow) Pipe(100);
		PipeR = new (std::nothrow) Pipe(100);
		//cout<<"sm3"<<endl;
		//pthread_t thread1,thread2;
		bigq_util *util1 = new (std::nothrow) bigq_util();
		util1->in = inPipeL;
		util1->out = PipeL;
		util1->run_len = nPages;
		util1->sort_order = &left;

		//cout<<"sm4"<<endl;
		bigq_util *util2 = new (std::nothrow) bigq_util();
		util2->in = inPipeR;
		util2->out = PipeR;
		util2->run_len = nPages;
		util2->sort_order = &right;

		pthread_t thread1, thread2;
		//cout<<"sm5"<<endl;
		pthread_create(&thread1, NULL, run_bigq, (void *)util1);

		pthread_create(&thread2, NULL, run_bigq, (void *)util2);

		int q1_status = PipeL->Remove(&rec1);
		int q2_status = PipeR->Remove(&rec2);
		//int q2_status;
		//cout<<"while true0"<<endl;
		int cnt = 0;

		Record mergedRecord;

		while (true)
		{
			//cout<<q1_status<<" "<<q2_status<<endl;
			if (q1_status && q2_status)
			{

				if (comp.Compare(&rec1, &left, &rec2, &right) < 0)
				{
					//cout<<"p1\n";
					q1_status = PipeL->Remove(&rec1);
					//cout<<cnt++<<endl;
					continue;
				}
				else if (comp.Compare(&rec1, &left, &rec2, &right) > 0)
				{
					//cout<<"p2\n";
					q2_status = PipeR->Remove(&rec2);
					continue;
				}
				else
				{
					//cout<<cnt++<<endl;

					//Compute cross product
					Record *temp = new (std::nothrow) Record();
					temp->Consume(&rec2);
					vector<Record *> rec_vector;

					while (q2_status && !comp.Compare(&rec1, &left, temp, &right))
					{
						//cout<<"2\n";
						//cout<<cnt++<<endl;
						rec_vector.push_back(temp);
						temp = new (std::nothrow) Record();
						q2_status = PipeR->Remove(temp);
					}

					if (q2_status)
						rec2.Consume(temp);

					delete temp;
					temp = NULL;
					//if(!rec1.bits) cout<<"1 culprit\n";
					//if(!rec_vector.front()->bits) cout<<"2 culprit\n";
					while (q1_status && !comp.Compare(&rec1, &left, rec_vector.front(),
													  &right))
					{

						for (int i = 0; i < rec_vector.size(); i++)
						{
							if (comp.Compare(&rec1, rec_vector[i], literal,
											 selOp))
							{
								//cout<<"1"<<endl;
								if (!join_calc_done)
								{
									numAttsLeft = ((((int *)(rec1.bits))[1]) / sizeof(int)) - 1;
									numAttsRight =
										((((int *)(rec_vector[i]->bits))[1]) / sizeof(int)) - 1;
									numAttsToKeep = numAttsLeft + numAttsRight;

									attsToKeep =
										new (std::nothrow) int[numAttsToKeep];
									int k;
									for (k = 0; k < numAttsLeft; k++)
									{
										attsToKeep[k] = k;
									}

									startOfRight = k;

									for (int l = 0; l < numAttsRight;
										 l++, k++)
									{
										attsToKeep[k] = l;
									}
									join_calc_done = 1;
								}

								mergedRecord.MergeRecords(&rec1, rec_vector[i],
														  numAttsLeft, numAttsRight, attsToKeep,
														  numAttsToKeep, startOfRight);
								outPipe->Insert(&mergedRecord);
								//cout<<"pipe written\n";
							}
						}
						q1_status = PipeL->Remove(&rec1);
						//cout<<cnt++<<endl;
					}

					for (int i = 0; i < rec_vector.size(); i++)
					{
						delete rec_vector[i];
					}

					rec_vector.empty();
				}
			}

			else
			{
				break;
			}
		}
		//cout<<"if done\n";
		delete PipeL;
		delete PipeR;
		delete attsToKeep;
		//return;
	}
	else
	{
		//Block Nested Loop Join

		Page p;
		int count = 0;
		vector<Record *> rec_vector1;
		vector<Record *> rec_vector2;
		int rec1_status = 0;
		int file_ready = 0;
		Record rec1, rec2;
		Record *temp;
		File f;
		f.Open(0, "temp.bin");
		int cur_page = 0;
		int scan_ptr = 0;
		int pipe2_status = 0;
		//exit(1);
		int cnt = 0, cnt2 = 0;
		//while (inPipeL->Remove(&rec1)) {
		//	cout<<cnt++<<endl;
		//}
		//exit(1);
		while (true)
		{
			//cout<<"rec1_status: "<<rec1_status<<endl;
			//cout<<"count: "<<count<<endl;
			if ((rec1_status = inPipeL->Remove(&rec1)) && count < (nPages - 1))
			{
				//cout<<"j1\n";
				//if(!rec1.bits) cout<<"fedrer\n";
				//				cout << "cnt: " << cnt2++ << endl;
				//cout<<"rec1_status: "<<rec1_status<<endl;
				if (!p.Append(&rec1))
				{
					//cout<<"j2\n";
					//cout<<"numrecs: "<<p.numRecs<<endl;
					temp = new (std::nothrow) Record();
					while (p.GetFirst(temp))
					{
						//cout<<"cnt: "<<cnt++<<endl;
						//Schema s("catalog","supplier");
						//temp->Print(&s);
						//if(!temp->bits) cout<<"federer\n";
						rec_vector1.push_back(temp);
						temp = new Record();
						//if(!temp) cout<<"no mem cre"<<endl;
					}
					//if(!rec_vector1[0]->bits) cout<<"woah"<<endl;

					delete temp;
					temp = NULL;
					p.Append(&rec1);
					//p.EmptyItOut();
					count++;
					//					cout << "count: " << count << endl;
				}
			}
			else
			{
				cnt += rec_vector1.size();
				//cout<<"recs: "<<cnt<<endl;
				//cout<<"rec1_status: "<<rec1_status<<endl;
				//				if (file_ready)
				//					cout << "file is ready\n";
				//cout<<"j3"<<endl;
				int scan_done = 0, scan_ptr = 0;
				Page scan_page;
				//cnt=0;

				if (count < nPages - 1)
				{
					//cout<<"in here\n";
					temp = new (std::nothrow) Record();
					while (p.GetFirst(temp))
					{
						//if(!temp->bits)	cout<<"nadal"<<endl;
						rec_vector1.push_back(temp);
						temp = new (std::nothrow) Record();
					}
					delete temp;
					temp = NULL;
				}

				while (true)
				{
					if (!file_ready)
					{
						//First time when relation2 file not ready
						//int cnt=0;
						while ((pipe2_status = inPipeR->Remove(&rec2)) && scan_page.Append(&rec2))
						{
							//cout<<cnt++<<endl;
							//cout<<"j4"<<endl;
						}

						//vector<Record*> temp_rec_vector;
						Page temp_page;
						Record *temp1;
						//cout<<"pip2 stat"<<pipe2_status<<endl;
						//cout<<"cnt: "<<cnt<<endl;
						temp = new (std::nothrow) Record();
						temp1 = new (std::nothrow) Record();
						while (scan_page.GetFirst(temp))
						{
							temp1->Copy(temp);
							rec_vector2.push_back(temp);
							temp_page.Append(temp1);
							temp = new (std::nothrow) Record();
							temp1 = new (std::nothrow) Record();
						}
						delete temp;
						delete temp1;
						temp = NULL;
						temp1 = NULL;

						//cnt+=rec_vector2.size();
						//cout<<"temp: "<<cnt<<endl;
						f.AddPage(&temp_page, cur_page++);
						temp_page.EmptyItOut();
						scan_page.EmptyItOut(); //
					}
					else
					{
						//cout<<"flen: "<<f.GetLength()<<endl;
						//Scanning if relation2's file ready
						if (scan_ptr < f.GetLength() - 1)
						{
							f.GetPage(&scan_page, scan_ptr++);
							temp = new (std::nothrow) Record();
							while (scan_page.GetFirst(temp))
							{
								rec_vector2.push_back(temp);
								temp = new (std::nothrow) Record();
							}
							delete temp;
							temp = NULL;
							scan_page.EmptyItOut();
							//cnt+=rec_vector2.size();
							//cout<<"temp: "<<cnt<<endl;
						}
						else
							scan_done = 1;
					}

					//cout<<"flen: "<<f.GetLength()<<endl;

					//					else {
					//						count=0;
					//						p.Append(&rec1);
					//					}

					cnt += rec_vector2.size();
					//cout<<"recs1: "<<rec_vector1.size()<<endl;
					//cout<<"recs2: "<<rec_vector2.size()<<endl;

					for (int i = 0; i < rec_vector1.size(); i++)
					{
						for (int j = 0; j < rec_vector2.size(); j++)
						{
							//							if(!rec_vector1[i]->bits) {
							//								cout<<"i: "<<i<<endl;
							//								cout<<"culprit1"<<endl;
							//							}
							//if(!rec_vector2[j]->bits) cout<<"culprit2"<<endl;
							if (comp.Compare(rec_vector1[i], rec_vector2[j],
											 literal, selOp))
							{
								//cout<<"j6"<<endl;
								Record mergedRecord;
								if (!join_calc_done)
								{
									numAttsLeft =
										((((int *)(rec_vector1[i]->bits))[1]) / sizeof(int)) - 1;
									numAttsRight =
										((((int *)(rec_vector2[j]->bits))[1]) / sizeof(int)) - 1;
									numAttsToKeep = numAttsLeft + numAttsRight;

									attsToKeep =
										new (std::nothrow) int[numAttsToKeep];
									int k;
									for (k = 0; k < numAttsLeft; k++)
									{
										attsToKeep[k] = k;
									}

									startOfRight = k;

									for (int l = 0; l < numAttsRight;
										 l++, k++)
									{
										attsToKeep[k] = l;
									}
									join_calc_done = 1;
								}

								mergedRecord.MergeRecords(rec_vector1[i],
														  rec_vector2[j], numAttsLeft,
														  numAttsRight, attsToKeep, numAttsToKeep,
														  startOfRight);
								outPipe->Insert(&mergedRecord);
								//cout<<"pipe written\n";
							}
						}
						//delete rec_vector1[i];
						//rec_vector1[i]=NULL;
					}

					for (int i = 0; i < rec_vector2.size(); i++)
					{
						delete rec_vector2[i];
						rec_vector2[i] = NULL;
					}

					//rec_vector1.clear();
					rec_vector2.clear();

					//cout<<"pipe2_status :"<<pipe2_status<<"file_ready: "<<file_ready<<endl;
					if (!pipe2_status && !file_ready)
					{
						file_ready = 1;
						break;
					}
					else if (!file_ready)
					{
						//						temp= new (std::nothrow) Record();
						//						temp->Consume(&rec2);
						//						rec_vector2.push_back(temp);
						//						temp=NULL;
						scan_page.Append(&rec2);
					}

					if (scan_done)
					{
						//scan_ptr=0;
						break;
					}
				}

				for (int i = 0; i < rec_vector1.size(); i++)
				{
					delete rec_vector1[i];
					rec_vector1[i] = NULL;
				}
				//cout<<"recs1: "<<rec_vector1.size()<<endl;
				//cout<<"recs2: "<<cnt<<endl;

				rec_vector1.clear();

				if (count >= (nPages - 1))
				{
					p.Append(&rec1);
					cnt++;
					count = 0;

					//continue;
				}
				else
					break;

				//if(!rec1_status) break;
			}
			//cout<<"cnt2: "<<cnt2++<<endl;
		}

		f.Close();			//Tempfile close
		remove("temp.bin"); //Deleting the temp file
	}
	//delete PipeR;

	outPipe->ShutDown();*/

	/*
	int leftint, rightint;
	OrderMaker leftsortorder, rightsortorder;
	Record *recleft = new Record();
	Record *recright = new Record();
	Record *recjoin = new Record();
	ComparisonEngine comp;
	Pipe *leftPipe = new Pipe(100);
	Pipe *rightPipe = new Pipe(100);
	vector<Record *> vecLeft, vecRight;

	cout << "working  start" << endl;

	if (selOp->GetSortOrders(leftsortorder, rightsortorder))
	{
		BigQ bigqleft(*inPipeL, *leftPipe, leftsortorder, 10);
		BigQ bigqright(*inPipeR, *rightPipe, rightsortorder, 10);
		cout << "working if" << endl;
		leftPipe->Remove(recleft);
		rightPipe->Remove(recright);
		bool leftflag, rightflag = false;
		leftint = ((int *)recleft->bits)[1] / sizeof(int) - 1;
		rightint = ((int *)recright->bits)[1] / sizeof(int) - 1;
		int attsArr[leftint + rightint];
		for (int i = 0; i < leftint; i++)
			attsArr[i] = i;
		for (int i = 0; i < rightint; i++)
			attsArr[leftint + i] = i;

		while (true)
		{
			if (comp.Compare(recleft, &leftsortorder, recright, &rightsortorder) < 0)
			{
				if (leftPipe->Remove(recleft) != 1)
					break;
			}
			else if (comp.Compare(recleft, &leftsortorder, recright, &rightsortorder) > 0)
			{
				if (rightPipe->Remove(recright) != 1)
					break;
			}
			else
			{
				leftflag = false;
				rightflag = false;

				while (true)
				{
					Record *dummy = new Record;
					dummy->Consume(recleft);
					vecLeft.push_back(dummy);
					if (leftPipe->Remove(recleft) != 1)
					{
						leftflag = true;
						break;
					}
					if (comp.Compare(dummy, recleft, &leftsortorder) != 0)
						break;
				}
				while (true)
				{
					Record *dummy = new Record;
					dummy->Consume(recright);
					vecRight.push_back(dummy);
					if (rightPipe->Remove(recright) != 1)
					{
						rightflag = true;
						break;
					}
					if (comp.Compare(dummy, recright, &rightsortorder) != 0)
						break;
				}

				for (int i = 0; i < vecLeft.size(); i++)
				{
					for (int j = 0; j < vecRight.size(); j++)
					{
						recjoin->MergeRecords(vecLeft.at(i), vecRight.at(j), leftint, rightint, attsArr, leftint + rightint, leftint);
						outPipe->Insert(recjoin);
					}
				}
				vecLeft.clear();
				vecRight.clear();

				if (leftflag || rightflag)
					break;
			}
		}
}
else
{
	cout << "working else" << endl;
	while (inPipeR->Remove(recright) == 1)
	{
		Record tempRec;
		tempRec.Consume(recright);
		vecRight.push_back(&tempRec);
	}
	inPipeL->Remove(recleft);
	leftint = ((int *)recleft->bits)[1] / sizeof(int) - 1;
	rightint = ((int *)vecRight.at(0)->bits)[1] / sizeof(int) - 1;

	int attsArr[leftint + rightint];
	for (int i = 0; i < leftint; i++)
		attsArr[i] = i;
	for (int i = 0; i < rightint; i++)
		attsArr[leftint + i] = i;
	while (inPipeL->Remove(recleft) == 1)
	{
		for (int j = 0; j < vecRight.size(); j++)
		{
			recjoin->MergeRecords(recleft, vecRight.at(j), leftint, rightint, attsArr, leftint + rightint, leftint);
			outPipe->Insert(recjoin);
		}
	}
}
outPipe->ShutDown();*/
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
	Record rec;
	Record prev_rec;
	ComparisonEngine comp;

	OrderMaker myOrder(mySchema);

	Pipe *in;
	Pipe *out;
	bigq_util *util1;
	pthread_t bigq_thread;
	in = new (std::nothrow) Pipe(100);
	out = new (std::nothrow) Pipe(100);
	util1 = new bigq_util();
	util1->in = in;
	util1->out = out;
	util1->sort_order = &myOrder;
	// Hard coding to 1 for now
	util1->run_len = 1;
	pthread_create(&bigq_thread, NULL, run_bigq, (void *)util1);

	//Write all the records to BigQ first
	//Get the records from outPipe and then compare with the previously fetched records
	//If it is same, then don't write that record to outPipe
	while (inPipe->Remove(&rec))
	{
		in->Insert(&rec);
	}

	in->ShutDown();
	bool first_time = true;
	while (out->Remove(&rec))
	{
		if (first_time)
		{
			//No Previous record to comapare with. Store current as prev_rec for next iteration
			prev_rec.Copy(&rec);
			first_time = false;
		}
		else
		{
			if (!comp.Compare(&rec, &prev_rec, &myOrder))
			{
				//Prev Record same as current
				//Don't write to outPipe, No need to copy rec to prev_rec
			}
			else
			{
				outPipe->Insert(&prev_rec);
				prev_rec.Copy(&rec);
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
	long reccnt = 0;
	// loop through all of the attributes
	int n = mySchema->GetNumAtts();
	Attribute *atts = mySchema->GetAtts();
	while (inPipe->Remove(&temp))
	{
		reccnt++;
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
	cout << "\nRecord count written:" << reccnt << "\n";
}

void WriteOut::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void WriteOut::Use_n_Pages(int n)
{
	this->nPages = n;
}