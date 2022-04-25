#pragma once

#if BUILDING_LIBZEF && __GNUC__ >= 4
#define LIBZEF_DLL_EXPORTED __attribute__((__visibility__("default")))
/* #pragma message "Building for exported DLL (gcc)" */
#elif BUILDING_LIBZEF && defined _MSC_VER
#define LIBZEF_DLL_EXPORTED __declspec(dllexport)
/* #pragma message "Building for exported DLL (MSVC)" */
#elif defined _MSC_VER
#define LIBZEF_DLL_EXPORTED __declspec(dllimport)
/* #pragma message "Building to import DLL (MSVC)" */
#else
#define LIBZEF_DLL_EXPORTED
/* #pragma message "Building to import DLL (gcc)" */
#endif
