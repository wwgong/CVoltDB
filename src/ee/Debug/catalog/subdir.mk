################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../catalog/authprogram.cpp \
../catalog/catalog.cpp \
../catalog/cataloginteger.cpp \
../catalog/catalogtype.cpp \
../catalog/cluster.cpp \
../catalog/column.cpp \
../catalog/columnref.cpp \
../catalog/commandlog.cpp \
../catalog/connector.cpp \
../catalog/connectortableinfo.cpp \
../catalog/constraint.cpp \
../catalog/constraintref.cpp \
../catalog/database.cpp \
../catalog/deployment.cpp \
../catalog/group.cpp \
../catalog/groupref.cpp \
../catalog/host.cpp \
../catalog/index.cpp \
../catalog/materializedviewinfo.cpp \
../catalog/partition.cpp \
../catalog/planfragment.cpp \
../catalog/procedure.cpp \
../catalog/procparameter.cpp \
../catalog/program.cpp \
../catalog/site.cpp \
../catalog/snapshotschedule.cpp \
../catalog/statement.cpp \
../catalog/stmtparameter.cpp \
../catalog/systemsettings.cpp \
../catalog/table.cpp \
../catalog/user.cpp \
../catalog/userref.cpp 

OBJS += \
./catalog/authprogram.o \
./catalog/catalog.o \
./catalog/cataloginteger.o \
./catalog/catalogtype.o \
./catalog/cluster.o \
./catalog/column.o \
./catalog/columnref.o \
./catalog/commandlog.o \
./catalog/connector.o \
./catalog/connectortableinfo.o \
./catalog/constraint.o \
./catalog/constraintref.o \
./catalog/database.o \
./catalog/deployment.o \
./catalog/group.o \
./catalog/groupref.o \
./catalog/host.o \
./catalog/index.o \
./catalog/materializedviewinfo.o \
./catalog/partition.o \
./catalog/planfragment.o \
./catalog/procedure.o \
./catalog/procparameter.o \
./catalog/program.o \
./catalog/site.o \
./catalog/snapshotschedule.o \
./catalog/statement.o \
./catalog/stmtparameter.o \
./catalog/systemsettings.o \
./catalog/table.o \
./catalog/user.o \
./catalog/userref.o 

CPP_DEPS += \
./catalog/authprogram.d \
./catalog/catalog.d \
./catalog/cataloginteger.d \
./catalog/catalogtype.d \
./catalog/cluster.d \
./catalog/column.d \
./catalog/columnref.d \
./catalog/commandlog.d \
./catalog/connector.d \
./catalog/connectortableinfo.d \
./catalog/constraint.d \
./catalog/constraintref.d \
./catalog/database.d \
./catalog/deployment.d \
./catalog/group.d \
./catalog/groupref.d \
./catalog/host.d \
./catalog/index.d \
./catalog/materializedviewinfo.d \
./catalog/partition.d \
./catalog/planfragment.d \
./catalog/procedure.d \
./catalog/procparameter.d \
./catalog/program.d \
./catalog/site.d \
./catalog/snapshotschedule.d \
./catalog/statement.d \
./catalog/stmtparameter.d \
./catalog/systemsettings.d \
./catalog/table.d \
./catalog/user.d \
./catalog/userref.d 


# Each subdirectory must supply rules for building sources it contributes
catalog/%.o: ../catalog/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


