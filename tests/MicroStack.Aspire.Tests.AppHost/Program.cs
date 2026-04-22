// Copyright (c) MicroStack contributors. All rights reserved.
// Licensed under the MIT License.

var builder = DistributedApplication.CreateBuilder(args);

builder.AddMicroStack("microstack")
    .WithServices("s3,sqs,sns");

builder.Build().Run();
