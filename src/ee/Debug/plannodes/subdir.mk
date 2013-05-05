################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../plannodes/SchemaColumn.cpp \
../plannodes/abstractjoinnode.cpp \
../plannodes/abstractoperationnode.cpp \
../plannodes/abstractplannode.cpp \
../plannodes/abstractscannode.cpp \
../plannodes/aggregatenode.cpp \
../plannodes/deletenode.cpp \
../plannodes/distinctnode.cpp \
../plannodes/indexscannode.cpp \
../plannodes/insertnode.cpp \
../plannodes/limitnode.cpp \
../plannodes/materializenode.cpp \
../plannodes/nestloopindexnode.cpp \
../plannodes/nestloopnode.cpp \
../plannodes/orderbynode.cpp \
../plannodes/plannodefragment.cpp \
../plannodes/plannodeutil.cpp \
../plannodes/projectionnode.cpp \
../plannodes/receivenode.cpp \
../plannodes/sendnode.cpp \
../plannodes/seqscannode.cpp \
../plannodes/unionnode.cpp \
../plannodes/updatenode.cpp 

OBJS += \
./plannodes/SchemaColumn.o \
./plannodes/abstractjoinnode.o \
./plannodes/abstractoperationnode.o \
./plannodes/abstractplannode.o \
./plannodes/abstractscannode.o \
./plannodes/aggregatenode.o \
./plannodes/deletenode.o \
./plannodes/distinctnode.o \
./plannodes/indexscannode.o \
./plannodes/insertnode.o \
./plannodes/limitnode.o \
./plannodes/materializenode.o \
./plannodes/nestloopindexnode.o \
./plannodes/nestloopnode.o \
./plannodes/orderbynode.o \
./plannodes/plannodefragment.o \
./plannodes/plannodeutil.o \
./plannodes/projectionnode.o \
./plannodes/receivenode.o \
./plannodes/sendnode.o \
./plannodes/seqscannode.o \
./plannodes/unionnode.o \
./plannodes/updatenode.o 

CPP_DEPS += \
./plannodes/SchemaColumn.d \
./plannodes/abstractjoinnode.d \
./plannodes/abstractoperationnode.d \
./plannodes/abstractplannode.d \
./plannodes/abstractscannode.d \
./plannodes/aggregatenode.d \
./plannodes/deletenode.d \
./plannodes/distinctnode.d \
./plannodes/indexscannode.d \
./plannodes/insertnode.d \
./plannodes/limitnode.d \
./plannodes/materializenode.d \
./plannodes/nestloopindexnode.d \
./plannodes/nestloopnode.d \
./plannodes/orderbynode.d \
./plannodes/plannodefragment.d \
./plannodes/plannodeutil.d \
./plannodes/projectionnode.d \
./plannodes/receivenode.d \
./plannodes/sendnode.d \
./plannodes/seqscannode.d \
./plannodes/unionnode.d \
./plannodes/updatenode.d 


# Each subdirectory must supply rules for building sources it contributes
plannodes/%.o: ../plannodes/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


