#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <string.h>


int main()
{
	int map[1500];
	for(int i = 0 ; i < 1500; map[i++] = i);
	
	printf("%d", map[150]);		
    return(0);
}

