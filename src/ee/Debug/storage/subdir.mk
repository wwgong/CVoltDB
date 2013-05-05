################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../storage/ConstraintFailureException.cpp \
../storage/CopyOnWriteContext.cpp \
../storage/CopyOnWriteIterator.cpp \
../storage/DataConflictException.cpp \
../storage/MaterializedViewMetadata.cpp \
../storage/PersistentTableStats.cpp \
../storage/PersistentTableUndoDeleteAction.cpp \
../storage/PersistentTableUndoEscrowUpdateAction.cpp \
../storage/PersistentTableUndoInsertAction.cpp \
../storage/PersistentTableUndoUpdateAction.cpp \
../storage/RecoveryContext.cpp \
../storage/StreamedTableStats.cpp \
../storage/TableCatalogDelegate.cpp \
../storage/TableStats.cpp \
../storage/TempTableLimits.cpp \
../storage/TupleBlock.cpp \
../storage/TupleStreamWrapper.cpp \
../storage/constraintutil.cpp \
../storage/persistenttable.cpp \
../storage/streamedtable.cpp \
../storage/table.cpp \
../storage/tablefactory.cpp \
../storage/tableutil.cpp \
../storage/temptable.cpp 

OBJS += \
./storage/ConstraintFailureException.o \
./storage/CopyOnWriteContext.o \
./storage/CopyOnWriteIterator.o \
./storage/DataConflictException.o \
./storage/MaterializedViewMetadata.o \
./storage/PersistentTableStats.o \
./storage/PersistentTableUndoDeleteAction.o \
./storage/PersistentTableUndoEscrowUpdateAction.o \
./storage/PersistentTableUndoInsertAction.o \
./storage/PersistentTableUndoUpdateAction.o \
./storage/RecoveryContext.o \
./storage/StreamedTableStats.o \
./storage/TableCatalogDelegate.o \
./storage/TableStats.o \
./storage/TempTableLimits.o \
./storage/TupleBlock.o \
./storage/TupleStreamWrapper.o \
./storage/constraintutil.o \
./storage/persistenttable.o \
./storage/streamedtable.o \
./storage/table.o \
./storage/tablefactory.o \
./storage/tableutil.o \
./storage/temptable.o 

CPP_DEPS += \
./storage/ConstraintFailureException.d \
./storage/CopyOnWriteContext.d \
./storage/CopyOnWriteIterator.d \
./storage/DataConflictException.d \
./storage/MaterializedViewMetadata.d \
./storage/PersistentTableStats.d \
./storage/PersistentTableUndoDeleteAction.d \
./storage/PersistentTableUndoEscrowUpdateAction.d \
./storage/PersistentTableUndoInsertAction.d \
./storage/PersistentTableUndoUpdateAction.d \
./storage/RecoveryContext.d \
./storage/StreamedTableStats.d \
./storage/TableCatalogDelegate.d \
./storage/TableStats.d \
./storage/TempTableLimits.d \
./storage/TupleBlock.d \
./storage/TupleStreamWrapper.d \
./storage/constraintutil.d \
./storage/persistenttable.d \
./storage/streamedtable.d \
./storage/table.d \
./storage/tablefactory.d \
./storage/tableutil.d \
./storage/temptable.d 


# Each subdirectory must supply rules for building sources it contributes
storage/%.o: ../storage/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


