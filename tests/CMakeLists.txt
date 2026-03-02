find_package(GTest CONFIG REQUIRED)

file(GLOB_RECURSE SURGE_TEST_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
)

if(SURGE_TEST_SOURCES)
    add_executable(surge_tests ${SURGE_TEST_SOURCES})

    target_link_libraries(surge_tests
        PRIVATE
            surge_core
            GTest::gtest_main
    )

    target_include_directories(surge_tests PRIVATE
        ${CMAKE_SOURCE_DIR}/src
    )

    include(GoogleTest)
    gtest_discover_tests(surge_tests)
endif()
